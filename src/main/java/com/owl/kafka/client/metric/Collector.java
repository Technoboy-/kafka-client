package com.owl.kafka.client.metric;

import com.owl.kafka.client.util.NamedThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class Collector {

    private final ScheduledExecutorService executor;

    private final Monitor monitor;

    public Collector(Monitor monitor){
        this.monitor = monitor;
        executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("monitor-thread"));

    }

    public void start(){
        executor.scheduleAtFixedRate(() -> monitor.stat(), 2, 1, TimeUnit.SECONDS);
    }
}
