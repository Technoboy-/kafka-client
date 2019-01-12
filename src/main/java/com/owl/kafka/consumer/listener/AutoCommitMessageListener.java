package com.owl.kafka.consumer.listener;

import com.owl.kafka.consumer.Record;

/**
 * @Author: Tboy
 */
public interface AutoCommitMessageListener<K, V> extends MessageListener<K, V> {

    void onMessage(Record<K, V> record);
}
