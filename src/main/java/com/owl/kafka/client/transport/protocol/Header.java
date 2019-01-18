package com.owl.kafka.client.transport.protocol;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class Header implements Serializable {

    private final String topic;

    private final int partition;

    private final long offset;

    private int repost;

    public Header(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public void setRepost(int repost) {
        this.repost = repost;
    }

    public int getRepost() {
        return repost;
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
