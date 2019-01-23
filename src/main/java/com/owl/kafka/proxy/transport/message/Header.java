package com.owl.kafka.proxy.transport.message;

import com.owl.kafka.proxy.service.PullStatus;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class Header implements Serializable {

    private String topic;

    private int partition;

    private long offset;

    private long msgId;

    private byte repost;

    private byte pullStatus;

    public Header() {
    }

    public Header(byte pullStatus) {
        this.pullStatus = pullStatus;
    }

    public Header(long msgId) {
        this.msgId = msgId;
    }

    public Header(String topic, int partition, long offset, long msgId) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.msgId = msgId;
        this.repost = (byte)1;
        this.pullStatus = PullStatus.FOUND.getStatus();
    }

    public Header(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public long getMsgId() {
        return msgId;
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

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setMsgId(long msgId) {
        this.msgId = msgId;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "Header{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", msgId=" + msgId +
                ", repost=" + repost +
                ", pullStatus=" + pullStatus +
                '}';
    }
}
