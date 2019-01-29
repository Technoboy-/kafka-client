package com.owl.kafka.client.proxy.transport.alloc;

import java.nio.ByteBuffer;

/**
 * @Author: Tboy
 */
public interface ByteBufferPool {

    ByteBuffer allocate(int capacity);

    void release(ByteBuffer buffer);

    ByteBufferPool DEFAULT = new SimpleByteBufferPool();
}
