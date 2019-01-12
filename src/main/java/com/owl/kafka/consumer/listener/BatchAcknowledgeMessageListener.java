package com.owl.kafka.consumer.listener;

import com.owl.kafka.consumer.Record;

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
