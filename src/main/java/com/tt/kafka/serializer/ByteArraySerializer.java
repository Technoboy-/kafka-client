package com.tt.kafka.serializer;

/**
 * @Author: Tboy
 */
public class ByteArraySerializer implements Serializer<byte[]>{

    public byte[] serialize(byte[] data) {
        return data;
    }

    @Override
    public byte[] deserialize(byte[] src, Class<byte[]> clazz) {
        return src;
    }

}
