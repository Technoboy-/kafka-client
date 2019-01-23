package com.owl.kafka.client.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Tboy
 */
public class NamedThreadFactory implements ThreadFactory {

    private final AtomicInteger increment = new AtomicInteger(1);

    private final String name;

    public NamedThreadFactory(String name){
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, name + "-" + increment.getAndIncrement());
        t.setDaemon(true);
        return t;
    }
}
