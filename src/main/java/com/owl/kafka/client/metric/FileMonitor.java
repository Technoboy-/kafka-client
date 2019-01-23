package com.owl.kafka.client.metric;


/**
 * @Author: Tboy
 */
public class FileMonitor implements Monitor {

    private final Stat stat;

    private OutputLog log;

    private Collector collector;

    public FileMonitor(String path, boolean enable){
        stat = new Stat();
        log = new OutputLog(path);
        if(enable){
            collector = new Collector(this);
            collector.start();
        }
    }

    @Override
    public void recordProduceSendCount(int count) {
        stat.recordProduceSendCount(count);
    }

    @Override
    public void recordProduceSendTime(long time) {
        stat.recordProduceSendTime(time);
    }

    @Override
    public void recordProduceSendError(int count) {
        stat.recordProduceSendError(count);
    }

    @Override
    public void recordConsumeRecvCount(int count) {
        stat.recordConsumeRecvCount(count);
    }

    @Override
    public void recordConsumeProcessCount(long count) {
        stat.recordConsumeProcessCount(count);
    }

    @Override
    public void recordConsumeProcessTime(long time) {
        stat.recordConsumeProcessTime(time);
    }

    @Override
    public void recordConsumeProcessErrorCount(long count) {
        stat.recordConsumeProcessErrorCount(count);
    }

    @Override
    public void recordConsumePollCount(int count) {
        stat.recordConsumePollCount(count);
    }

    @Override
    public void recordConsumePollTime(long time) {
        stat.recordConsumePollTime(time);
    }

    @Override
    public void recordCommitCount(long count) {
        stat.recordCommitCount(count);
    }

    @Override
    public void recordCommitTime(long time) {
        stat.recordCommitTime(time);
    }

    @Override
    public void recordConsumeHandlerCount(int count) {
        stat.recordConsumeHandlerCount(count);
    }

    @Override
    public void stat() {
        log.info(stat.getStat());
    }
}
