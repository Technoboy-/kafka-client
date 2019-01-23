package com.owl.kafka.client.consumer.assignor;

import com.owl.kafka.client.consumer.exceptions.TopicNotExistException;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * @Author: Tboy
 */
public class CheckTopicStickyAssignor extends StickyAssignor {

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        for (Map.Entry<String, Subscription> entry: subscriptions.entrySet()) {
            for (String topic: entry.getValue().topics()) {
                if(partitionsPerTopic.get(topic) == null){
                   throw new TopicNotExistException("topic [ " + topic + " ] not exist");
                }
            }
        }
        return super.assign(partitionsPerTopic, subscriptions);
    }
}
