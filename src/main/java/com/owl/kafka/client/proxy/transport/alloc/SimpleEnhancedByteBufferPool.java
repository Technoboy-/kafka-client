package com.owl.kafka.client.proxy.transport.alloc;

import com.owl.kafka.client.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @Author: Tboy
 */
public class SimpleEnhancedByteBufferPool{

    private static final TreeMap<Integer, Arena> heapBuffers = new TreeMap<>();

    private static final TreeMap<Integer, Arena> directBuffers = new TreeMap<>();

    private static final boolean direct = Boolean.valueOf(System.getProperty("allocator.direct.buffer", "false"));

    private static final int MAX_ALLOCATE_SIZE = 16 * 1024 * 1024;

    static{
        //use for heartbeat, pull request, response etc packet that contain not bigger datas
        int chunk_1k = 1024; //1k
        int chunk_1m = chunk_1k * 1024;
        int chunk_2m = chunk_1m * 2;
        int chunk_4m = chunk_2m * 2;
        int chunk_8m = chunk_4m * 2;
        int chunk_16m = chunk_8m * 2;
        //1k
        Arena heapArena1K = new Arena(false, chunk_1k, 320);
        Arena directArena1K = new Arena(true, chunk_1k, 320);
        heapBuffers.put(chunk_1k, heapArena1K);
        directBuffers.put(chunk_1k, directArena1K);

        //1m
        Arena heapArena1M = new Arena(false,  chunk_1m, 160);
        Arena directArena1M = new Arena(true,  chunk_1m, 160);
        heapBuffers.put(chunk_1m, heapArena1M);
        directBuffers.put(chunk_1m, directArena1M);
        //2m
        Arena heapArena2M = new Arena(false,  chunk_2m, 80);
        Arena directArena2M = new Arena(true,  chunk_2m, 80);
        heapBuffers.put(chunk_2m, heapArena2M);
        directBuffers.put(chunk_2m, directArena2M);
        //4m
        Arena heapArena4M = new Arena(false,  chunk_4m, 40);
        Arena directArena4M = new Arena(true,  chunk_4m, 40);
        heapBuffers.put(chunk_4m, heapArena4M);
        directBuffers.put(chunk_4m, directArena4M);
        //8m
        Arena heapArena8M = new Arena(false,  chunk_8m, 20);
        Arena directArena8M = new Arena(true,  chunk_8m, 20);
        heapBuffers.put(chunk_8m, heapArena8M);
        directBuffers.put(chunk_8m, directArena8M);
        //16m
        Arena heapArena16M = new Arena(false,  chunk_16m, 10);
        Arena directArena16M = new Arena(true,  chunk_16m, 10);
        heapBuffers.put(chunk_16m, heapArena16M);
        directBuffers.put(chunk_16m, directArena16M);

    }

    private static final TreeMap<Integer, Arena> getBuffer(boolean direct) {
        return direct ? directBuffers : heapBuffers;
    }

    private static final ByteBuffer allocateDirectly(int capacity, boolean direct){
        return direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    }

    public synchronized ByteBuffer allocate(int capacity) {
        Preconditions.checkArgument(capacity < MAX_ALLOCATE_SIZE, "allocate too large buffer : " + capacity + " , max : " + MAX_ALLOCATE_SIZE);
        TreeMap<Integer, Arena> treeMap = getBuffer(direct);
        ByteBuffer buffer = null;
        int originalCapacity = capacity;
        while(capacity < MAX_ALLOCATE_SIZE){
            Map.Entry<Integer, Arena> entry = treeMap.ceilingEntry(capacity);
            buffer = entry.getValue().getBuffer();
            if(buffer == null){
                capacity = capacity << 1;
            }
        }
        if(buffer == null){
            buffer = allocateDirectly(originalCapacity, direct);
        }
        assert buffer != null;
        buffer.flip();
        return buffer;
    }

    public synchronized void release(ByteBuffer buffer) {
        TreeMap<Integer, Arena> treeMap = getBuffer(buffer.isDirect());
        Map.Entry<Integer, Arena> entry = treeMap.ceilingEntry(buffer.capacity());
        if(entry != null){
            Arena arena = entry.getValue();
            arena.release(buffer);
        }
    }

    private static final class Arena implements Comparable<Arena>{

        private final Queue<ByteBuffer> chunkQueue;

        private final int chunkSize;

        private final boolean direct;

        public Arena(boolean direct, int chunkSize, int size){
            this.direct = direct;
            this.chunkSize = chunkSize;
            this.chunkQueue = new ArrayBlockingQueue<>(size);
            for(int i = 0; i < size; i++){
                this.chunkQueue.offer(direct ? ByteBuffer.allocateDirect(chunkSize) : ByteBuffer.allocate(chunkSize));
            }
        }

        public ByteBuffer getBuffer() {
            return chunkQueue.poll();
        }

        public void release(ByteBuffer buffer){
            boolean offer = chunkQueue.offer(buffer);
            if(!offer){
                buffer = null;
            }
        }

        public int getChunkSize() {
            return chunkSize;
        }

        @Override
        public int compareTo(Arena other) {
            return (this.chunkSize > other.getChunkSize()) ? 1 : (this.chunkSize < other.getChunkSize()) ? -1 : 0;
        }

    }

}
