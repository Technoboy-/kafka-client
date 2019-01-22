package com.owl.kafka.client.transport.protocol;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class Message implements Serializable {

    private byte[] header;

    private byte[] key;

    private byte[] value;

    public byte[] getHeader() {
        return header;
    }

    public void setHeader(byte[] header) {
        this.header = header;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public long getLength(){
        return header.length + key.length + value.length;
    }

    @Override
    public String toString() {
        return "Message{" +
                "headerLength=" + header.length +
                ", keyLength==" + key.length +
                ", valueLength==" + value.length +
                '}';
    }
}
