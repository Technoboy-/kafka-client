package com.owl.kafka.client.producer;


import com.owl.kafka.client.serializer.Serializer;

import java.util.HashMap;

/**
 * http://kafka.apache.org/0110/documentation.html#producerconfigs
 * @Author: Tboy
 */
public class ProducerConfig extends HashMap<String, Object> {

    public ProducerConfig(String bootstrapServers){
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * 服务器列表
     */
    private String bootstrapServers;

    /**
     * key的序列化
     */
    private Serializer keySerializer;

    /**
     * value的序列化
     */
    private Serializer valueSerializer;

    /**
     * ack类型 all，-1，0，1
     */
    private String acks = "1";

    /**
     * send内存缓存区大小
     */
    private long bufferMemory = 33554432;

    /**
     * send失败重试次数
     */
    private int retries = 0;

    /**
     * request batch的大小
     */
    private int batchSize = 16384;

    /**
     * request batch驻留时间，超过改大小后发送该request batch
     */
    private int lingerMs = 0;

    /**
     * 每个连接同时能保留的未响应请求数
     */
    private int maxInflightRequestPerConnection = 5;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        put("bootstrap.servers", bootstrapServers);

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

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
        put("acks", acks);
    }

    public long getBufferMemory() {
        return bufferMemory;
    }

    public void setBufferMemory(long bufferMemory) {
        this.bufferMemory = bufferMemory;
        put("buffer.memory", bufferMemory);
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
        put("retries", retries);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        put("batch.size", batchSize);
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
        put("linger.ms", lingerMs);
    }

    public int getMaxInflightRequestPerConnection() {
        return maxInflightRequestPerConnection;
    }

    public void setMaxInflightRequestPerConnection(int maxInflightRequestPerConnection) {
        this.maxInflightRequestPerConnection = maxInflightRequestPerConnection;
        put("max.in.flight.requests.per.connection", maxInflightRequestPerConnection);
    }
}
