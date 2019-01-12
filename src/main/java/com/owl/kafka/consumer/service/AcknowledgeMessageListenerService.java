package com.owl.kafka.consumer.service;

import com.owl.kafka.consumer.listener.AcknowledgeMessageListener;
import com.owl.kafka.consumer.listener.MessageListener;
import com.owl.kafka.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.metric.MonitorImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerService<K, V> extends RebalanceAcknowledgeMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgeMessageListenerService.class);

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private final AcknowledgeMessageListener<K, V> messageListener;

    public AcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, MessageListener<K, V> messageListener) {
        super(consumer);
        this.consumer = consumer;
        this.messageListener = (AcknowledgeMessageListener)messageListener;
        MonitorImpl.getDefault().recordConsumeHandlerCount(1);
    }

    @Override
    public void onMessage(final ConsumerRecord<byte[], byte[]> record) {
        long now = System.currentTimeMillis();
        try {
            final Record<K, V> r = consumer.toRecord(record);
            messageListener.onMessage(r, new AcknowledgeMessageListener.Acknowledgment() {
                @Override
                public void acknowledge() {
                    AcknowledgeMessageListenerService.super.acknowledge(r);
                }
            });
        } catch (Throwable ex) {
            MonitorImpl.getDefault().recordConsumeProcessErrorCount(1);
            LOG.error("onMessage error", ex);
        } finally {
            MonitorImpl.getDefault().recordConsumeProcessCount(1);
            MonitorImpl.getDefault().recordConsumeProcessTime(System.currentTimeMillis() - now);
        }
    }

    @Override
    public void close() {
        super.close();
        LOG.debug("AcknowledgeMessageListenerService stop.");
    }
}
