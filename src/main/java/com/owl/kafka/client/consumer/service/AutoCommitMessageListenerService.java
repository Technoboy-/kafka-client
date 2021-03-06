package com.owl.kafka.client.consumer.service;

import com.owl.kafka.client.consumer.listener.AutoCommitMessageListener;
import com.owl.kafka.client.consumer.listener.MessageListener;
import com.owl.kafka.client.consumer.DefaultKafkaConsumerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class AutoCommitMessageListenerService<K, V> extends RebalanceMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(AutoCommitMessageListenerService.class);

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private final AutoCommitMessageListener<K, V> messageListener;

    public AutoCommitMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, MessageListener<K, V> listener) {
        this.consumer = consumer;
        this.messageListener = (AutoCommitMessageListener)listener;
        consumer.getMetricsMonitor().recordConsumeHandlerCount(1);
    }

    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        long now = System.currentTimeMillis();
        try {
            this.messageListener.onMessage(consumer.toRecord(record));
        } catch (Throwable ex) {
            consumer.getMetricsMonitor().recordConsumeProcessErrorCount(1);
            LOG.error("onMessage error", ex);
        } finally {
            consumer.getMetricsMonitor().recordConsumeProcessCount(1);
            consumer.getMetricsMonitor().recordConsumeProcessTime(System.currentTimeMillis() - now);
        }
    }

    @Override
    public void close() {
        //NOP
    }
}
