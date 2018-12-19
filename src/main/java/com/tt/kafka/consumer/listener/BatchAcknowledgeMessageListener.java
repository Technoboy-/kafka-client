package com.tt.kafka.consumer.listener;

import com.tt.kafka.consumer.Record;

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
