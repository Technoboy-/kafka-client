package com.owl.kafka.client.proxy.transport.alloc;

/**
 * @Author: Tboy
 */
public class PoolArena {

    private int chunkSize = 700;

    private int defaultPageSize = 2048;

    private int subpageOverflowMask = ~(defaultPageSize - 1);

    public void allocate(){

    }

    //小于chunkSize且大于512字节，倍数扩展。小于chunkSize小于512字节，结果为临近16倍数.
    private int normalizeCapacity(int capacity){
        if(capacity < 0){
            throw new IllegalArgumentException("capacity : " + capacity + " (expected : 0+)");
        }

        if(capacity >= chunkSize){
            return capacity;
        }

        if(!isTiny(capacity)){
            int normalizedCapacity = capacity;
            normalizedCapacity --;
            normalizedCapacity |= normalizedCapacity >>>  1;
            normalizedCapacity |= normalizedCapacity >>>  2;
            normalizedCapacity |= normalizedCapacity >>>  4;
            normalizedCapacity |= normalizedCapacity >>>  8;
            normalizedCapacity |= normalizedCapacity >>> 16;
            normalizedCapacity ++;

            if (normalizedCapacity < 0) {
                normalizedCapacity >>>= 1;
            }

            return normalizedCapacity;
        }

        if ((capacity & 15) == 0) {
            return capacity;
        }

        return (capacity & ~15) + 16;
    }

    private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);

    private int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        for (int i = maxOrder; i > 0; i --) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(String.format(
                        "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    //
    private int validateAndCalculatePageShifts(int pageSize) {
        if (pageSize < defaultPageSize) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: " + defaultPageSize + ")");
        }

        if ((pageSize & pageSize - 1) != 0) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
        }

        // Logarithm base 2. At this point we know that pageSize is a power of two.
        return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
    }

    // 512k
    private boolean isTiny(int capacity){
        return (capacity & 0xFFFFFE00) == 0;
    }

    //capacity < pageSize
    boolean isTinyOrSmall(int normCapacity) {
        return (normCapacity & subpageOverflowMask) == 0;
    }

    public static void main(String[] args) {
        System.out.println(512 >>> 4);
    }
}
