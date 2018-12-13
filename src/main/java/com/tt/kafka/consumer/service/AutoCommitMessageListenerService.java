package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.listener.AutoCommitMessageListener;
import com.tt.kafka.metric.Monitor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class AutoCommitMessageListenerService<K, V> extends ReblanceMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(AutoCommitMessageListenerService.class);

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private final AutoCommitMessageListener<K, V> messageListener;

    public AutoCommitMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, AutoCommitMessageListener<K, V> listener) {
        this.consumer = consumer;
        this.messageListener = listener;
        Monitor.getInstance().recordConsumeHandlerCount(1);
    }

    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        long now = System.currentTimeMillis();
        try {
            this.messageListener.onMessage(consumer.toRecord(record));
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
        //NOP
    }
}
