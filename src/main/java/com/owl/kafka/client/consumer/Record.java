package com.owl.kafka.client.consumer;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: tboy
 */
public class Record<K, V> implements Serializable {

    public static final Record EMPTY = new Record<>(-1, "", -1, -1, null, null, -1);

    private long msgId;

    private final String topic;

    private final int partition;

    private final long offset;

    private final K key;

    private final V value;

    private final long timestamp;

    public Record(long msgId, String topic, int partition, long offset, K key, V value, long timestamp) {
        this.msgId = msgId;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public long getMsgId() {
        return msgId;
    }

    public void setMsgId(long msgId) {
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

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Record{" +
                "msgId=" + msgId +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record<?, ?> record = (Record<?, ?>) o;
        return msgId == record.msgId &&
                partition == record.partition &&
                offset == record.offset &&
                timestamp == record.timestamp &&
                Objects.equals(topic, record.topic) &&
                Objects.equals(key, record.key) &&
                Objects.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgId, topic, partition, offset, key, value, timestamp);
    }
}
