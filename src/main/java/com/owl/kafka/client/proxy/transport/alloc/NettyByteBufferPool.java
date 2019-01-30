package com.owl.kafka.client.proxy.transport.alloc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * @Author: Tboy
 */
public class NettyByteBufferPool implements ByteBufferPool {

    private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    public ByteBuf allocate(int capacity) {
        return allocator.buffer(capacity);
    }

    public  void release(ByteBuf buffer) {
        buffer.release();
    }

}
