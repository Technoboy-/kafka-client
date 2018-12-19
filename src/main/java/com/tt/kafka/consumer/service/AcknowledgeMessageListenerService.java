package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.Record;
import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.metric.Monitor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerService<K, V> extends BaseAcknowledgeMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgeMessageListenerService.class);

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private final AcknowledgeMessageListener<K, V> messageListener;

    public AcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, AcknowledgeMessageListener<K, V> messageListener) {
        super(consumer);
        this.consumer = consumer;
        this.messageListener = messageListener;
        Monitor.getInstance().recordConsumeHandlerCount(1);
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
            Monitor.getInstance().recordConsumeProcessErrorCount(1);
            LOG.error("onMessage error", ex);
        } finally {
            Monitor.getInstance().recordConsumeProcessCount(1);
            Monitor.getInstance().recordConsumeProcessTime(System.currentTimeMillis() - now);
        }
    }

    @Override
    public void close() {
        super.close();
        LOG.debug("AcknowledgeMessageListenerService stop.");
    }
}
