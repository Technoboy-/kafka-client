package com.owl.kafka.client.consumer.listener;

import com.owl.kafka.client.consumer.Record;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface BatchAcknowledgeMessageListener<K, V> extends MessageListener<K, V> {

    void onMessage(List<Record<K, V>> records, Acknowledgment acknowledgment);

    interface Acknowledgment{
        void acknowledge();
    }
}
