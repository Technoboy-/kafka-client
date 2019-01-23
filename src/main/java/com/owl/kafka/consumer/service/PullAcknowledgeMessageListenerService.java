package com.owl.kafka.consumer.service;

import com.owl.kafka.client.ClientConfigs;
import com.owl.kafka.client.service.ProcessQueue;
import com.owl.kafka.client.service.PullStatus;
import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.transport.message.Message;
import com.owl.kafka.client.transport.message.Header;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.consumer.listener.AcknowledgeMessageListener;
import com.owl.kafka.consumer.listener.MessageListener;
import com.owl.kafka.metric.MonitorImpl;
import com.owl.kafka.util.NamedThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public class PullAcknowledgeMessageListenerService<K, V> implements MessageListenerService<K, V>{

    private static final Logger LOG = LoggerFactory.getLogger(PullAcknowledgeMessageListenerService.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("PullAcknowledgeMessageListenerService-thread"));

    private final int parallelism = ClientConfigs.I.getParallelismNum();

    private final int consumeBatchSize = 2;

    private final ThreadPoolExecutor consumeExecutor = new ThreadPoolExecutor(parallelism, parallelism, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    private final AcknowledgeMessageListener<K, V> messageListener;

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private Connection connection;

    public PullAcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, MessageListener<K, V> messageListener) {
        this.consumer = consumer;
        this.messageListener = (AcknowledgeMessageListener)messageListener;
        MonitorImpl.getDefault().recordConsumeHandlerCount(1);
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        //
    }

    public void onMessage(Connection connection, List<Message> messages) {
        this.connection = connection;
        if(messages.size() < consumeBatchSize){
            ConsumeRequest consumeRequest = new ConsumeRequest(messages);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch(RejectedExecutionException ex){
                consumeLater(consumeRequest);
            }
        } else{
            for(int total = 0; total < messages.size(); ){
                List<Message> msgList = new ArrayList<>(consumeBatchSize);
                for(int i = 0; i < consumeBatchSize; i++, total++){
                    if(total < messages.size()){
                        msgList.add(messages.get(total));
                    } else{
                        break;
                    }
                }
                ConsumeRequest consumeRequest = new ConsumeRequest(msgList);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < messages.size(); total++) {
                        msgList.add(messages.get(total));
                    }
                    this.consumeLater(consumeRequest);
                }
            }
        }
    }

    private void consumeLater(ConsumeRequest request){
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                consumeExecutor.submit(request);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private void sendBack(Message message){
        try {
            connection.send(Packets.sendBackReq(message));
        } catch (ChannelInactiveException e) {
            consumeLater(new ConsumeRequest(Arrays.asList(message)));
        }
    }

    class ConsumeRequest implements Runnable{

        private List<Message> messages;

        public ConsumeRequest(List<Message> messages){
            this.messages = messages;
        }

        @Override
        public void run() {
            for(Message message : messages){
                long now = System.currentTimeMillis();
                long msgId = -1;
                try {
                    Header header = message.getHeader();
                    PullStatus pullStatus = PullStatus.of(header.getPullStatus());
                    msgId = header.getMsgId();
                    ProcessQueue.I.put(msgId, message);
                    switch (pullStatus){
                        case FOUND:
                            ConsumerRecord record = new ConsumerRecord(header.getTopic(), header.getPartition(), header.getOffset(), message.getKey(), message.getValue());
                            final Record<K, V> r = consumer.toRecord(record);
                            r.setMsgId(header.getMsgId());
                            messageListener.onMessage(r, new AcknowledgeMessageListener.Acknowledgment() {
                                @Override
                                public void acknowledge() {
                                    ProcessQueue.I.remove(header.getMsgId());
                                }
                            });
                            break;
                        case NO_NEW_MSG:
                            LOG.debug("no new msg");
                            break;
                    }
                } catch (Throwable ex) {
                    MonitorImpl.getDefault().recordConsumeProcessErrorCount(1);
                    LOG.error("onMessage error", ex);
                    ProcessQueue.I.remove(msgId);
                    sendBack(message);
                } finally {
                    MonitorImpl.getDefault().recordConsumeProcessCount(1);
                    MonitorImpl.getDefault().recordConsumeProcessTime(System.currentTimeMillis() - now);
                }
            }
        }
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
    }
}
