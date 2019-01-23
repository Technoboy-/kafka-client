package com.owl.kafka.client;

import com.owl.kafka.client.consumer.ConsumerConfig;
import com.owl.kafka.client.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.client.consumer.KafkaConsumer;
import com.owl.kafka.client.producer.DefaultKafkaProducerImpl;
import com.owl.kafka.client.producer.ProducerConfig;
import com.owl.kafka.client.producer.KafkaProducer;

/**
 * @Author: Tboy
 */
public class OwlKafkaClient {

    public static <K, V> KafkaConsumer<K, V> createConsumer(ConsumerConfig configs)
    {
        return new DefaultKafkaConsumerImpl<>(configs);
    }

    public static <K, V> KafkaProducer<K, V> createProducer(ProducerConfig configs){
        return new DefaultKafkaProducerImpl<>(configs);
    }
}
