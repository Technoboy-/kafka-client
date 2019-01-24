package com.owl.kafka.client.consumer.service;

import com.owl.kafka.client.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.client.consumer.Record;
import com.owl.kafka.client.consumer.listener.AcknowledgeMessageListener;
import com.owl.kafka.client.consumer.listener.MessageListener;
import com.owl.kafka.client.metric.MonitorImpl;
import com.owl.kafka.client.proxy.ClientConfigs;
import com.owl.kafka.client.proxy.service.OffsetStore;
import com.owl.kafka.client.proxy.service.PullStatus;
import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.message.Header;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.util.Packets;
import com.owl.kafka.client.util.CollectionUtils;
import com.owl.kafka.client.util.NamedThreadFactory;
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

    private final int consumeBatchSize = ClientConfigs.I.getConsumeBatchSize();

    private final ThreadPoolExecutor consumeExecutor = new ThreadPoolExecutor(parallelism, parallelism, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private final AcknowledgeMessageListener<K, V> messageListener;

    private Connection connection;

    private final OffsetStore offsetStore = OffsetStore.I;

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
        final List<Message> newMessages = filter(messages);
        if(CollectionUtils.isEmpty(newMessages)){
            LOG.debug("no new msg");
            return;
        }
        offsetStore.storeOffset(newMessages);
        if(newMessages.size() < consumeBatchSize){
            ConsumeRequest consumeRequest = new ConsumeRequest(newMessages);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch(RejectedExecutionException ex){
                consumeLater(consumeRequest);
            }
        } else{
            for(int total = 0; total < newMessages.size(); ){
                List<Message> msgList = new ArrayList<>(consumeBatchSize);
                for(int i = 0; i < consumeBatchSize; i++, total++){
                    if(total < newMessages.size()){
                        msgList.add(newMessages.get(total));
                    } else{
                        break;
                    }
                }
                ConsumeRequest consumeRequest = new ConsumeRequest(msgList);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < newMessages.size(); total++) {
                        msgList.add(newMessages.get(total));
                    }
                    this.consumeLater(consumeRequest);
                }
            }
        }
    }

    private List<Message> filter(List<Message> messages){
        List<Message> newMessages = new ArrayList<>(messages.size());
        for(Message message : messages){
            PullStatus pullStatus = PullStatus.of(message.getHeader().getPullStatus());
            if(PullStatus.FOUND == pullStatus){
                newMessages.add(message);
            }
        }
        return newMessages;
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
                try {
                    Header header = message.getHeader();
                    ConsumerRecord record = new ConsumerRecord(header.getTopic(), header.getPartition(), header.getOffset(), message.getKey(), message.getValue());
                    final Record<K, V> r = consumer.toRecord(record);
                    r.setMsgId(header.getMsgId());
                    messageListener.onMessage(r, new AcknowledgeMessageListener.Acknowledgment() {
                        @Override
                        public void acknowledge() {
                            processConsumeResult(r);
                        }
                    });
                } catch (Throwable ex) {
                    MonitorImpl.getDefault().recordConsumeProcessErrorCount(1);
                    LOG.error("onMessage error", ex);
                    sendBack(message);
                } finally {
                    MonitorImpl.getDefault().recordConsumeProcessCount(1);
                    MonitorImpl.getDefault().recordConsumeProcessTime(System.currentTimeMillis() - now);
                }
            }
        }
    }

    private void processConsumeResult(final Record<K, V> record){
        offsetStore.updateOffset(connection, record.getMsgId());
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
    }
}
