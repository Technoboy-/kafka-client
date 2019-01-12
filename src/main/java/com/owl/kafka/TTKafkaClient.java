package com.owl.kafka;

import com.owl.kafka.consumer.ConsumerConfig;
import com.owl.kafka.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.consumer.KafkaConsumer;
import com.owl.kafka.producer.DefaultKafkaProducerImpl;
import com.owl.kafka.producer.ProducerConfig;
import com.owl.kafka.producer.KafkaProducer;

/**
 * @Author: Tboy
 */
public class TTKafkaClient {

    public static <K, V> KafkaConsumer<K, V> createConsumer(ConsumerConfig configs)
    {
        return new DefaultKafkaConsumerImpl<>(configs);
    }

    public static <K, V> KafkaProducer<K, V> createProducer(ProducerConfig configs){
        return new DefaultKafkaProducerImpl<>(configs);
    }
}
