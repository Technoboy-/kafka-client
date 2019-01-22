package com.owl.kafka.consumer.service;

import com.owl.kafka.client.ClientConfigs;
import com.owl.kafka.client.service.ProcessQueue;
import com.owl.kafka.client.service.PullStatus;
import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.transport.message.Message;
import com.owl.kafka.client.transport.protocol.Header;
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

import java.util.List;
import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public class PullAcknowledgeMessageListenerService<K, V> implements MessageListenerService<K, V>{

    private static final Logger LOG = LoggerFactory.getLogger(PullAcknowledgeMessageListenerService.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("PullAcknowledgeMessageListenerService-thread"));

    private final int parallelism = ClientConfigs.I.getParallelismNum();

    private final ThreadPoolExecutor consumeExcutor = new ThreadPoolExecutor(parallelism, parallelism, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

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
        //TODO 是batch消费还是多线程消费多个消息
        for(Message message : messages){
            this.onMessage(message);
        }
    }

    private void onMessage(Message message) {
        consumeExcutor.submit(new ConsumeRequest(message));
    }

    private void sendBack(Message message){
        try {
            connection.send(Packets.toSendBackPacket(message));
        } catch (ChannelInactiveException e) {
            scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    onMessage(message);
                }
            }, 3, TimeUnit.SECONDS);
        }
    }

    class ConsumeRequest implements Runnable{

        private Message message;

        public ConsumeRequest(Message message){
            this.message = message;
        }

        @Override
        public void run() {
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

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
    }
}
