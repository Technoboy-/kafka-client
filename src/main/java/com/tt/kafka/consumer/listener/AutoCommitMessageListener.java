package com.tt.kafka.consumer.listener;

import com.tt.kafka.consumer.Record;

/**
 * @Author: Tboy
 */
public interface AutoCommitMessageListener<K, V> extends MessageListener<K, V> {

    void onMessage(Record<K, V> record);
}
