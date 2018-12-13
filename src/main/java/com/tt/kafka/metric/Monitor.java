package com.tt.kafka.metric;

import com.tt.kafka.util.NamedThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class Monitor {

    private ScheduledExecutorService executor;

    private final Stat stat;

    private OutputLog log;

    public Monitor(String path, String name, boolean isEnable){
        stat = new Stat();
        if(isEnable){
            log = new OutputLog(path, name);
            executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("monitor-thread"));
            executor.scheduleAtFixedRate(() -> log.info(stat.getStat()), 2, 1, TimeUnit.SECONDS);
        }
    }

    private static class MonitorHolder{
        private static final Monitor INSTANCE = new Monitor(Monitor.class.getResource(".").getPath(),"monitor.log", true);
    }

    public static Monitor getInstance() {
        return MonitorHolder.INSTANCE;
    }

    public void recordProduceSendCount(int count) {
        stat.recordProduceSendCount(count);
    }

    public void recordProduceSendTime(long time) {
        stat.recordProduceSendTime(time);
    }

    public void recordProduceSendError(int count) {
        stat.recordProduceSendError(count);
    }

    public void recordConsumeRecvCount(int count) {
        stat.recordConsumeRecvCount(count);
    }

    public void recordConsumeProcessCount(long count) {
        stat.recordConsumeProcessCount(count);
    }

    public void recordConsumeProcessTime(long time) {
        stat.recordConsumeProcessTime(time);
    }

    public void recordConsumeProcessErrorCount(long count) {
        stat.recordConsumeProcessErrorCount(count);
    }

    public void recordConsumePollCount(int count) {
        stat.recordConsumePollCount(count);
    }

    public void recordConsumePollTime(long time) {
        stat.recordConsumePollTime(time);
    }

    public void recordCommitCount(long count) {
        stat.recordCommitCount(count);
    }

    public void recordCommitTime(long time) {
        stat.recordCommitTime(time);
    }

    public void recordConsumeHandlerCount(int count) {
        stat.recordConsumeHandlerCount(count);
    }

}
