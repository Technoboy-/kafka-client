package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.listener.AutoCommitMessageListener;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.metric.MonitorImpl;
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
        MonitorImpl.getDefault().recordConsumeHandlerCount(1);
    }

    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        long now = System.currentTimeMillis();
        try {
            this.messageListener.onMessage(consumer.toRecord(record));
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
        //NOP
    }
}
