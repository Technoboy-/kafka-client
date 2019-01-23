package com.owl.kafka.client.consumer.listener;

import com.owl.kafka.client.consumer.Record;

/**
 * @Author: Tboy
 */
public interface AcknowledgeMessageListener<K, V> extends MessageListener<K, V> {

    void onMessage(Record<K, V> record, Acknowledgment acknowledgment);

    interface Acknowledgment{
        void acknowledge();
    }
}
