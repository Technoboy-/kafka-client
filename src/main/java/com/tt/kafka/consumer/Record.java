package com.tt.kafka.consumer;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: tboy
 */
public class Record<K, V> implements Serializable {

    private final String topic;

    private final int partition;

    private final long offset;

    private final K key;

    private final V value;

    private final long timestamp;

    public Record(String topic, int partition, long offset, K key, V value, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record<?, ?> record = (Record<?, ?>) o;
        return partition == record.partition &&
                offset == record.offset &&
                timestamp == record.timestamp &&
                Objects.equals(topic, record.topic) &&
                Objects.equals(key, record.key) &&
                Objects.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return "Record{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
