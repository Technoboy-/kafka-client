package com.owl.kafka.client.producer;


import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public interface KafkaProducer<K, V> {

    //
    SendResult sendSync(String topic, V value);

    SendResult sendSync(String topic, K key, V value);

    SendResult sendSync(String topic, Integer partition, K key, V value);

    //
    Future<SendResult> sendAsync(String topic, V value, Callback callback);

    Future<SendResult> sendAsync(String topic, K key, V value, Callback callback);

    Future<SendResult> sendAsync(String topic, Integer partition, K key, V value, Callback callback);

    void flush();

    void close();

    void close(long timeout, TimeUnit unit);
}
