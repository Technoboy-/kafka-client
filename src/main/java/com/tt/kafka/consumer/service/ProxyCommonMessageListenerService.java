package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.Record;
import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.metric.MonitorImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: jiwei.guo
 * @Date: 2019/1/5 5:50 PM
 */
abstract class ProxyCommonMessageListenerService<K, V> implements MessageListenerService<K, V>{

    protected final DefaultKafkaConsumerImpl<K, V> consumer;

    public ProxyCommonMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer) {
        this.consumer = consumer;
    }

    public Record<K, V> toRecord(ConsumerRecord<byte[], byte[]> record){
        return consumer.toRecord(record);
    }

}
