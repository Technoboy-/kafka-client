package com.owl.kafka.client.transport.protocol;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class Header implements Serializable {

    private String topic;

    private int partition;

    private long offset;

    private byte repost;

    private byte pullStatus;

    public Header(byte pullStatus) {
        this.pullStatus = pullStatus;
    }

    public Header(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public byte getPullStatus() {
        return pullStatus;
    }

    public void setPullStatus(byte pullStatus) {
        this.pullStatus = pullStatus;
    }

    public void setRepost(byte repost) {
        this.repost = repost;
    }

    public byte getRepost() {
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
