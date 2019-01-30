package com.owl.kafka.client.proxy.transport.alloc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * @Author: Tboy
 */
public interface ByteBufferPool {

    CompositeByteBuf compositeBuffer();

    ByteBuf allocate(int capacity);

    void release(ByteBuf buffer);

    ByteBufferPool DEFAULT = new NettyByteBufferPool();
}
