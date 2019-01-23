package com.owl.kafka.client.metric;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
public class Counter {

    private final AtomicLong counter = new AtomicLong(0);

    private volatile long pre = 0;

    public void increase(long inc){
        counter.addAndGet(inc);
    }

    public void increase(){
        counter.incrementAndGet();
    }

    public void decrease(){
        counter.decrementAndGet();
    }

    public long count(){
        long current = counter.get();
        long count = current - pre;
        pre = current;
        return count;
    }
}
