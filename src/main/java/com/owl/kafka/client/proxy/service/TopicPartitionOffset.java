package com.owl.kafka.client.proxy.service;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: Tboy
 */
public class TopicPartitionOffset implements Serializable, Comparable<TopicPartitionOffset> {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long msgId;

    public TopicPartitionOffset(String topic, int partition, long offset, long msgId){
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.msgId = msgId;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getMsgId() {
        return msgId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartitionOffset that = (TopicPartitionOffset) o;
        return getPartition() == that.getPartition() &&
                getOffset() == that.getOffset() &&
                Objects.equals(getTopic(), that.getTopic());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTopic(), getPartition(), getOffset());
    }

    @Override
    public String toString() {
        return "TopicPartitionOffset{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", msgId=" + msgId +
                '}';
    }

    @Override
    public int compareTo(TopicPartitionOffset o) {
        return this.partition > o.partition ? 1 : (this.partition < o.partition ? -1 : (this.offset > o.offset ? 1 : this.offset < o.offset ? -1 : 0) );
    }
}


