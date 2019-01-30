package com.owl.kafka.client.proxy.transport.alloc;

import io.netty.buffer.ByteBuf;

/**
 * @Author: Tboy
 */
public interface ByteBufferPool {

    ByteBuf allocate(int capacity);

    void release(ByteBuf buffer);

    ByteBufferPool DEFAULT = new NettyByteBufferPool();
}
