package com.owl.kafka.client.metric;


import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Tboy
 */
public abstract class FileMetricsMonitor implements MetricsMonitor {

    protected final Counter pollCounter = new Counter();
    protected final Counter pollTimeCounter  = new Counter();
    protected final Counter recvCounter  = new Counter();
    protected final Counter processCounter = new Counter();
    protected final Counter processTimeCounter = new Counter();
    protected final Counter processErrorCounter = new Counter();
    protected final Counter commitCounter = new Counter();
    protected final Counter commitTimeCounter = new Counter();
    protected final AtomicInteger handlerCounter = new AtomicInteger(0);
    //
    protected final String POLL_COUNT = "POLL_CNT";
    protected final String POLL_TIME = "POLL_TIME";
    protected final String RECV_COUNT = "RECV_CNT";
    protected final String PROCESS_COUNT = "PROC_CNT";
    protected final String PROCESS_TIME = "PROC_TIME";
    protected final String PROCESS_ERROR = "PROC_ERR";
    protected final String COMMIT_COUNT = "COMMIT_CNT";
    protected final String COMMIT_TIME = "COMMIT_TIME";
    protected final String HANDLER = "HANDLER";
    //
    protected final String SEND_COUNT = "SEND_CNT";
    protected final String SEND_TIME = "SEND_TIME";
    protected final String SEND_ERROR = "SEND_ERR";
    //
    protected final Counter sendCounter = new Counter();
    protected final Counter sendTimeCounter = new Counter();
    protected final Counter sendErrorCounter = new Counter();

    public FileMetricsMonitor(String name){
        MonitorManager monitorManager = new MonitorManager(name);
        monitorManager.register(getIMonitor());
        monitorManager.start();
    }

    public abstract IMonitor getIMonitor();

    @Override
    public void recordProduceSendCount(int count) {
        sendCounter.increase(count);
    }

    @Override
    public void recordProduceSendTime(long time) {
        sendTimeCounter.increase(time);
    }

    @Override
    public void recordProduceSendError(int count) {
        sendErrorCounter.increase(count);
    }

    @Override
    public void recordConsumeRecvCount(int count) {
        recvCounter.increase(count);
    }

    @Override
    public void recordConsumeProcessCount(long count) {
        processCounter.increase(count);
    }

    @Override
    public void recordConsumeProcessTime(long time) {
        processTimeCounter.increase(time);
    }

    @Override
    public void recordConsumeProcessErrorCount(long count) {
        processErrorCounter.increase(count);
    }

    @Override
    public void recordConsumePollCount(int count) {
        pollCounter.increase(count);
    }

    @Override
    public void recordConsumePollTime(long time) {
        pollTimeCounter.increase(time);
    }

    @Override
    public void recordCommitCount(long count) {
        commitCounter.increase(count);
    }

    @Override
    public void recordCommitTime(long time) {
        commitTimeCounter.increase(time);
    }

    @Override
    public void recordConsumeHandlerCount(int count) {
        handlerCounter.set(count);
    }
}
