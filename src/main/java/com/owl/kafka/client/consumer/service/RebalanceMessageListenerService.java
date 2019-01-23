package com.owl.kafka.client.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @Author: Tboy
 */
public abstract class RebalanceMessageListenerService<K, V> implements MessageListenerService<K, V>, ConsumerRebalanceListener{

    private static final Logger LOG = LoggerFactory.getLogger(RebalanceMessageListenerService.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.info("partition before rebalance : {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.info("partition after rebalance : {}", partitions);
    }
}
