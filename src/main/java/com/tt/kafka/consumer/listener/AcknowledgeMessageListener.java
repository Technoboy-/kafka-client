package com.tt.kafka.consumer.listener;

import com.tt.kafka.consumer.Record;

/**
 * @Author: Tboy
 */
public interface AcknowledgeMessageListener<K, V> extends MessageListener<K, V> {

    void onMessage(Record<K, V> record, Acknowledgment acknowledgment);

    interface Acknowledgment{
        void acknowledge();
    }
}
