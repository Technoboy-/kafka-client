package com.owl.kafka.client.proxy.transport.alloc;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * @Author: Tboy
 */
public class SimpleByteBufferPool {

    private static final TreeMap<Key, ByteBuffer> heapBuffers = new TreeMap<>();

    private static final TreeMap<Key, ByteBuffer> directBuffers = new TreeMap<>();

    private static final boolean direct = Boolean.valueOf(System.getProperty("allocator.direct.buffer", "false"));

    private static final TreeMap<Key, ByteBuffer> getBuffer(boolean direct) {
        return direct ? directBuffers : heapBuffers;
    }

    public synchronized ByteBuffer allocate(int capacity) {

        TreeMap<Key, ByteBuffer> treeMap = getBuffer(direct);

        Map.Entry<Key, ByteBuffer> entry = treeMap.ceilingEntry(new Key(capacity, 0));
        if (entry == null) {
            return direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
        }
        treeMap.remove(entry.getKey());
        ByteBuffer buffer = entry.getValue();
        buffer.flip();
        return buffer;
    }

    public synchronized void release(ByteBuffer buffer) {
        TreeMap<Key, ByteBuffer> treeMap = getBuffer(buffer.isDirect());
        while (true) {
            Key key = new Key(buffer.capacity(), System.nanoTime());
            if (!treeMap.containsKey(key)) {
                treeMap.put(key, buffer);
                return;
            }
        }
    }

    private static final class Key implements Comparable<Key> {
        private final int capacity;
        private final long insertionTime;

        Key(int capacity, long insertionTime) {
            this.capacity = capacity;
            this.insertionTime = insertionTime;
        }

        @Override
        public int compareTo(Key other) {
            return (this.capacity > other.capacity) ? 1 : (this.capacity < other.capacity) ? -1 :
                    (this.insertionTime > other.insertionTime ? 1 : (this.insertionTime < other.insertionTime ? -1 : 0));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return capacity == key.capacity &&
                    insertionTime == key.insertionTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(capacity, insertionTime);
        }
    }

}
