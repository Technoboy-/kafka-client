package com.tt.kafka.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @Author: Tboy
 */
public interface MessageListenerService<K, V> {

    void onMessage(ConsumerRecord<byte[], byte[]> record);

    void close();
}
