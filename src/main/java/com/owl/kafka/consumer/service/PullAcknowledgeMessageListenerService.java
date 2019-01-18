package com.owl.kafka.consumer.service;

import com.owl.kafka.client.service.ProcessQueue;
import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.consumer.listener.AcknowledgeMessageListener;
import com.owl.kafka.consumer.listener.MessageListener;
import com.owl.kafka.metric.MonitorImpl;
import com.owl.kafka.serializer.SerializerImpl;
import com.owl.kafka.util.NamedThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class PullAcknowledgeMessageListenerService<K, V> implements MessageListenerService<K, V>{

    private static final Logger LOG = LoggerFactory.getLogger(PullAcknowledgeMessageListenerService.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("PullAcknowledgeMessageListenerService-thread"));

    private final AcknowledgeMessageListener<K, V> messageListener;

    protected final DefaultKafkaConsumerImpl<K, V> consumer;

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

    public void onMessage(Connection connection, Packet packet) {
        this.connection = connection;
        this.onMessage(packet);
    }

    private void onMessage(Packet packet) {
        long now = System.currentTimeMillis();
        try {
            ProcessQueue.I.put(packet);
            Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(packet.getHeader(), Header.class);
            ConsumerRecord record = new ConsumerRecord(header.getTopic(), header.getPartition(), header.getOffset(), packet.getKey(), packet.getValue());
            final Record<K, V> r = consumer.toRecord(record);
            r.setMsgId(packet.getMsgId());
            messageListener.onMessage(r, new AcknowledgeMessageListener.Acknowledgment() {
                @Override
                public void acknowledge() {
                    ProcessQueue.I.remove(packet.getMsgId());
                }
            });
        } catch (Throwable ex) {
            MonitorImpl.getDefault().recordConsumeProcessErrorCount(1);
            LOG.error("onMessage error", ex);
            sendBack(packet);
        } finally {
            MonitorImpl.getDefault().recordConsumeProcessCount(1);
            MonitorImpl.getDefault().recordConsumeProcessTime(System.currentTimeMillis() - now);
        }
    }

    private void sendBack(Packet packet){
        try {
            connection.send(Packets.toSendBackPacket(packet));
        } catch (ChannelInactiveException e) {
            scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    onMessage(packet);
                }
            }, 3, TimeUnit.SECONDS);
        }
        ProcessQueue.I.remove(packet.getMsgId());
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
    }
}
