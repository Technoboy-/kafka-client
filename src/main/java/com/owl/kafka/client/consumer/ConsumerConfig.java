package com.owl.kafka.client.consumer;

import com.owl.kafka.client.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;

/**
 * @Author: Tboy
 */
public class ConsumerConfig extends HashMap<String, Object> {

    /**
     * subscribe模式 指定zookeeper服务器地址和命名空间
     * @param zookeeperServers
     * @param topic
     * @param groupId
     */
    public ConsumerConfig(String zookeeperServers, String namespace, String topic, String groupId) {
        this.zookeeperServers = zookeeperServers;
        this.zookeeperNamespace = namespace;
        this.topic = topic;
        this.groupId = groupId;
    }

    /**
     * assign模式，指定zookeeper服务器地址和命名空间
     * @param zookeeperServers
     * @param topicPartitions
     * @param groupId
     */
    public ConsumerConfig(String zookeeperServers, String namespace, Collection<TopicPartition> topicPartitions, String groupId) {
        this.zookeeperServers = zookeeperServers;
        this.zookeeperNamespace = namespace;
        this.topicPartitions = topicPartitions;
        this.groupId = groupId;
    }

    /**
     * subscribe模式
     * @param kafkaServers
     * @param topic
     * @param groupId
     */
    public ConsumerConfig(String kafkaServers, String topic, String groupId) {
        this.kafkaServers = kafkaServers;
        this.topic = topic;
        this.groupId = groupId;
    }

    /**
     * assign模式
     * @param kafkaServers
     * @param topicPartitions
     * @param groupId
     */
    public ConsumerConfig(String kafkaServers, Collection<TopicPartition> topicPartitions, String groupId) {
        this.kafkaServers = kafkaServers;
        this.topicPartitions = topicPartitions;
        this.groupId = groupId;
    }

    /**
     * kafka服务器列表
     */
    private String kafkaServers;

    /**
     * zookeeper服务器列表
     */
    private String zookeeperServers;

    /**
     * zookeeper的命名空间
     */
    private String zookeeperNamespace;

    /**
     * 消费主题
     */
    private String topic;

    /**
     * 消费主题分区
     */
    private Collection<TopicPartition> topicPartitions;

    /**
     * 消费组
     */
    private String groupId;

    /**
     * key的序列化
     */
    private Serializer keySerializer;

    /**
     * value的序列化
     */
    private Serializer valueSerializer;

    /**
     * 手动提交batch大小
     */
    private int acknowledgeCommitBatchSize = 10000;

    /**
     * 内部定时手动提交的间隔，单位s
     */
    private int acknowledgeCommitInterval = 30;

    /**
     * 默认offset重置策略，earliest，latest
     */
    private String autoOffsetReset = "latest";

    /**
     * 自动提交间隔时间，毫秒
     */
    private int autoCommitInterval = 5000;

    /**
     * 自动提交
     */
    private boolean autoCommit = false;

    /**
     * 分区有序，适用于自动提交和手动提交
     */
    private boolean partitionOrderly = false;

    /**
     * 自动提交下，分区无序的并发消费线程数
     */
    private int parallelism = 0;

    /**
     * poll线程每次的拉取超时时间
     */
    private long pollTimeout = 100;

    /**
     * 默认队列大小
     * 在多线程消费下(手动下的分区有序，自动下的多线程无序，自动下的分区有序)，poll线程会不断向队列中
     * 存放拉去到的消息，当业务方消费慢时，就会卡住poll线程，防止消息过度挤压。
     */
    private int handlerQueueSize = 100;

    /**
     * 手动提交下，批量消费的大小
     */
    private int batchConsumeSize = 200;

    /**
     * 手动提交下，批量消费的间隔时间, 单位秒
     */
    private int batchConsumeTime = 2;

    private boolean useProxy = false;

    private ProxyModel proxyModel = ProxyModel.PULL;

    public ProxyModel getProxyModel() {
        return proxyModel;
    }

    public void setProxyModel(ProxyModel proxyModel) {
        this.proxyModel = proxyModel;
    }

    public String getZookeeperServers() {
        return zookeeperServers;
    }

    public String getZookeeperNamespace() {
        return zookeeperNamespace;
    }

    public boolean isUseProxy() {
        return useProxy;
    }

    public void setUseProxy(boolean useProxy) {
        this.useProxy = useProxy;
    }

    public Collection<TopicPartition> getTopicPartitions() {
        return topicPartitions;
    }

    public int getBatchConsumeSize() {
        return batchConsumeSize;
    }

    public void setBatchConsumeSize(int batchConsumeSize) {
        this.batchConsumeSize = batchConsumeSize;
    }

    public int getBatchConsumeTime() {
        return batchConsumeTime;
    }

    public void setBatchConsumeTime(int batchConsumeTime) {
        this.batchConsumeTime = batchConsumeTime;
    }

    public int getHandlerQueueSize() {
        return handlerQueueSize;
    }

    public void setHandlerQueueSize(int handlerQueueSize) {
        this.handlerQueueSize = handlerQueueSize;
    }

    public long getPollTimeout() {
        return pollTimeout;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public boolean isPartitionOrderly() {
        return partitionOrderly;
    }

    public void setPartitionOrderly(boolean partitionOrderly) {
        this.partitionOrderly = partitionOrderly;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        put("enable.auto.commit", autoCommit);
    }

    public int getAcknowledgeCommitBatchSize() {
        return acknowledgeCommitBatchSize;
    }

    public void setAcknowledgeCommitBatchSize(int acknowledgeCommitBatchSize) {
        this.acknowledgeCommitBatchSize = acknowledgeCommitBatchSize;
    }

    public Serializer getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(Serializer keySerializer) {
        this.keySerializer = keySerializer;
    }

    public Serializer getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(Serializer valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public int getAcknowledgeCommitInterval() {
        return acknowledgeCommitInterval;
    }

    public void setAcknowledgeCommitInterval(int acknowledgeCommitInterval) {
        this.acknowledgeCommitInterval = acknowledgeCommitInterval;
    }

    public void setPollTimeout(long pollTimeout) {
        this.pollTimeout = pollTimeout;
    }


    public String getKafkaServers() {
        return kafkaServers;
    }

    public String getTopic() {
        return topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
        put("auto.offset.reset", autoOffsetReset);
    }

    public int getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public void setAutoCommitInterval(int autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
        put("auto.commit.interval.ms", autoCommitInterval);
    }

    public enum ProxyModel{
        PUSH,
        PULL;
    }
}
