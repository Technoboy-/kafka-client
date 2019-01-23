package com.owl.kafka.client.consumer;

import com.owl.kafka.client.consumer.listener.MessageListener;

/**
 * @Author: Tboy
 */
public interface KafkaConsumer<K, V> {

    /**
     * view DQL record
     * @param msgId
     * @return
     */
    Record<byte[], byte[]> view(long msgId);

    void setMessageListener(final MessageListener<K, V> messageListener);

    void start();

    void close();

}
