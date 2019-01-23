package com.owl.kafka.client.serializer;

import com.owl.kafka.client.util.Constants;

/**
 * @Author: Tboy
 */
public class StringSerializer implements Serializer<String> {

    @Override
    public byte[] serialize(String obj) {
        return obj.getBytes(Constants.UTF8);
    }

    @Override
    public String deserialize(byte[] src, Class<String> clazz) {
        return new String(src, Constants.UTF8);
    }
}
