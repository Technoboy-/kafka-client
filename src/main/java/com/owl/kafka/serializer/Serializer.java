package com.owl.kafka.serializer;

import com.owl.kafka.common.spi.SPI;

/**
 * @Author: Tboy
 */
@SPI("bytearray")
public interface Serializer<T> {

    byte[] serialize(T obj);

    T deserialize(byte[] src, Class<T> clazz);

}
