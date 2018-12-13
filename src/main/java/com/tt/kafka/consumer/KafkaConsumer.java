package com.tt.kafka.consumer;

import com.tt.kafka.consumer.listener.MessageListener;

/**
 * @Author: Tboy
 */
public interface KafkaConsumer<K, V> {

    void setMessageListener(final MessageListener<K, V> messageListener);

    void start();

    void close();

}
