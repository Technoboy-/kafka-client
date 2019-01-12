package com.owl.kafka.consumer;

import com.owl.kafka.consumer.listener.MessageListener;

/**
 * @Author: Tboy
 */
public interface KafkaConsumer<K, V> {

    void setMessageListener(final MessageListener<K, V> messageListener);

    void start();

    void close();

}
