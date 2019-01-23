package com.owl.kafka.client.serializer;

import com.owl.kafka.client.spi.SPI;

/**
 * @Author: Tboy
 */
@SPI("bytearray")
public interface Serializer<T> {

    byte[] serialize(T obj);

    T deserialize(byte[] src, Class<T> clazz);

}
