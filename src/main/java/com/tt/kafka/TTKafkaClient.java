package com.tt.kafka;

import com.tt.kafka.consumer.ConsumerConfig;
import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.KafkaConsumer;
import com.tt.kafka.producer.DefaultKafkaProducerImpl;
import com.tt.kafka.producer.ProducerConfig;
import com.tt.kafka.producer.KafkaProducer;

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
