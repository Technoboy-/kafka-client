package com.tt.kafka.client.transport.protocol;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class Header implements Serializable {

    private final String topic;

    private final int partition;

    private final long offset;

    public Header(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
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
}
