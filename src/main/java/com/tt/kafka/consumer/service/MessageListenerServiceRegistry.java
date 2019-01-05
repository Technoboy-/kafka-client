package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.ConsumerConfig;
import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.AutoCommitMessageListener;
import com.tt.kafka.consumer.listener.BatchAcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.util.CollectionUtils;

import java.lang.reflect.Constructor;

/**
 * @Author: Tboy
 */
@SuppressWarnings("all")
public class MessageListenerServiceRegistry<K, V> {

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private final MessageListener<K, V> messageListener;

    private MessageListenerService messageListenerService;

    private final String packageName = "com.tt.kafka.consumer.service.";

    public MessageListenerServiceRegistry(DefaultKafkaConsumerImpl<K, V> consumer, MessageListener<K, V> messageListener){
        this.consumer = consumer;
        this.messageListener = messageListener;
    }

    public void registry(MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
    }

    public MessageListenerService getMessageListenerService(boolean useReflection){
        if(this.messageListenerService != null){
            return this.messageListenerService;
        }
        if(useReflection){
            return getServiceByReflection();
        }
        ConsumerConfig configs = this.consumer.getConfigs();
        boolean partitionOrderly = configs.isPartitionOrderly();
        boolean isAssignTopicPartition = !CollectionUtils.isEmpty(configs.getTopicPartitions());
        boolean useProxy = configs.isUseProxy();
        int parallel = configs.getParallelism();
        if (messageListener instanceof AcknowledgeMessageListener) {
            if(useProxy){
                this.messageListenerService = new ProxyAcknowledgeMessageListenerService(this.consumer, messageListener);
            } else if (partitionOrderly && isAssignTopicPartition) {
                this.messageListenerService = new AssignPartitionOrderlyAcknowledgeMessageListenerService(this.consumer, messageListener);
            } else if(partitionOrderly){
                this.messageListenerService = new PartitionOrderlyAcknowledgeMessageListenerService(this.consumer, messageListener);
            } else if(isAssignTopicPartition){
                this.messageListenerService = new AssignAcknowledgeMessageListenerService(this.consumer, messageListener);
            } else{
                this.messageListenerService = new AcknowledgeMessageListenerService(this.consumer, messageListener);
            }
        } else if(messageListener instanceof BatchAcknowledgeMessageListener){
            if(useProxy){
                this.messageListenerService = new ProxyBatchAcknowledgeMessageListenerService(this.consumer, messageListener);
            } else if(isAssignTopicPartition){
                this.messageListenerService = new AssignBatchAcknowledgeMessageListenerService(this.consumer, messageListener);
            } else{
                this.messageListenerService = new BatchAcknowledgeMessageListenerService(this.consumer, messageListener);
            }
        } else if (messageListener instanceof AutoCommitMessageListener) {
            if(useProxy){
                this.messageListenerService = new ProxyAutoCommitMessageListenerService(this.consumer, messageListener);
            } else if(partitionOrderly){
                this.messageListenerService = new PartitionOrderlyAutoCommitMessageListenerService(this.consumer, messageListener);
            } else if(parallel <= 0){
                this.messageListenerService = new AutoCommitMessageListenerService(this.consumer, messageListener);
            } else{
                this.messageListenerService = new ConcurrentAutoCommitMessageListenerService(this.consumer, messageListener);
            }
        }
        return this.messageListenerService;
    }

    private MessageListenerService getServiceByReflection(){
        if(this.messageListenerService != null){
            return this.messageListenerService;
        }
        ConsumerConfig configs = this.consumer.getConfigs();
        boolean partitionOrderly = configs.isPartitionOrderly();
        boolean isAssignTopicPartition = !CollectionUtils.isEmpty(configs.getTopicPartitions());
        int parallel = configs.getParallelism();
        StringBuilder className = new StringBuilder(50);
        if (messageListener instanceof AcknowledgeMessageListener) {
            className.append(isAssignTopicPartition && partitionOrderly ? "AssignPartitionOrderly" : (partitionOrderly ? "PartitionOrderly" : (isAssignTopicPartition ? "Assign" : "")));
            className.append("AcknowledgeMessageListenerService");
        } else if(messageListener instanceof BatchAcknowledgeMessageListener){
            className.append(isAssignTopicPartition  ? "Assign" : "");
            className.append("BatchAcknowledgeMessageListenerService");
        } else if(messageListener instanceof AutoCommitMessageListener){
            className.append(partitionOrderly ? "PartitionOrderly" : (parallel > 0 ? "Concurrent" : ""));
            className.append("AutoCommitMessageListenerService");
        }
        this.messageListenerService = initializeClass(className.toString());
        return this.messageListenerService;
    }

    private MessageListenerService initializeClass(String className){
        try {
            Class<?> clazz = Class.forName(packageName + className, true, MessageListenerServiceRegistry.class.getClassLoader());
            Constructor<?> constructor = clazz.getConstructor(DefaultKafkaConsumerImpl.class, MessageListener.class);
            constructor.setAccessible(true);
            return (MessageListenerService)constructor.newInstance(this.consumer, this.messageListener);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
