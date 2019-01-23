package com.owl.kafka.client.metric;

/**
 * @Author: Tboy
 */
public class ConsoleMonitor implements Monitor {


    private Collector collector;

    public ConsoleMonitor(String path, boolean enable){
        if(enable){
            collector = new Collector(this);
            collector.start();
        }
    }

    @Override
    public void recordProduceSendCount(int count) {

    }

    @Override
    public void recordProduceSendTime(long time) {

    }

    @Override
    public void recordProduceSendError(int count) {

    }

    @Override
    public void recordConsumeRecvCount(int count) {

    }

    @Override
    public void recordConsumeProcessCount(long count) {

    }

    @Override
    public void recordConsumeProcessTime(long time) {

    }

    @Override
    public void recordConsumeProcessErrorCount(long count) {

    }

    @Override
    public void recordConsumePollCount(int count) {

    }

    @Override
    public void recordConsumePollTime(long time) {

    }

    @Override
    public void recordCommitCount(long count) {

    }

    @Override
    public void recordCommitTime(long time) {

    }

    @Override
    public void recordConsumeHandlerCount(int count) {

    }

    @Override
    public void stat() {

    }
}
