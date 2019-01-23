package com.owl.kafka.client.consumer.service;

import com.owl.kafka.client.consumer.DefaultKafkaConsumerImpl;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @Author: Tboy
 */
abstract class RebalanceAcknowledgeMessageListenerService<K, V> extends CommonAcknowledgeMessageListenerService<K, V> implements ConsumerRebalanceListener {

    private static final Logger LOG = LoggerFactory.getLogger(RebalanceAcknowledgeMessageListenerService.class);

    public RebalanceAcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer){
        super(consumer);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.info("partition before rebalance : {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.info("partition after rebalance : {}", partitions);
    }


    public void close(){
        super.close();
    }

}
