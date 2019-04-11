package com.owl.kafka.client.metric;

import com.owl.kafka.client.util.NamedThreadFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
public class MonitorManager implements Runnable{

    private final ScheduledExecutorService executor;

    private final OutputLog outputLog;

    private IMonitor monitor;

    private final AtomicLong titleCounter = new AtomicLong(-1);

    private final Map<String, String> titlesMap = new HashMap<>();

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    public MonitorManager(String name){
        this.outputLog = new OutputLog(name);
        this.executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("metrics-monitor-thread"));
    }

    public void register(IMonitor iMonitor){
        this.monitor = iMonitor;
    }

    public void start(){
        isStarted.compareAndSet(false, true);
        this.executor.schedule(this, 2, TimeUnit.SECONDS);
    }

    public void close(){
        this.isStarted.compareAndSet(true, false);
        this.executor.shutdown();
    }

    private void monitor(){
        StringBuilder builder = new StringBuilder(100);
        titleCounter.incrementAndGet();
        if(titleCounter.get() % 20 == 0){
            for(String title : monitor.getStatTitles()){
                builder.append(title).append("\t");
            }
            outputLog.info(builder.toString());
            return;
        }
        for(String title : monitor.getStatTitles()){
            int tabCount = (title.length() >>> 3) + 1;
            builder.append(titlesMap.get(title));
            for (int i = 0; i < tabCount; i++) {
                builder.append("\t");
            }
        }
        outputLog.info(builder.toString());
    }

    @Override
    public void run() {
        while(isStarted.get()){
            this.monitor.configure(titlesMap);
            this.monitor();
            try {
                TimeUnit.MILLISECONDS.sleep(this.monitor.getMonitorInterval());
            } catch (InterruptedException e) { }
        }
    }


}
