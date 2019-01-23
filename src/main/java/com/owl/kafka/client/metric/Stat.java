package com.owl.kafka.client.metric;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: tboy
 */
class Stat{

    private final Counter sendCounter;
    private final Counter sendTimeCounter;
    private final Counter sendErrorCounter;
    private final Counter pollCounter;
    private final Counter pollTimeCounter;
    private final Counter recvCounter;
    private final Counter processCounter;
    private final Counter processTimeCounter;
    private final Counter processErrorCounter;
    private final Counter commitCounter;
    private final Counter commitTimeCounter;
    private final Counter handlerCounter;

    public Stat(){
        this.sendCounter = new Counter();
        this.sendTimeCounter = new Counter();
        this.sendErrorCounter = new Counter();
        this.pollCounter = new Counter();
        this.pollTimeCounter = new Counter();
        this.recvCounter = new Counter();
        this.processCounter = new Counter();
        this.processTimeCounter = new Counter();
        this.processErrorCounter = new Counter();
        this.commitCounter = new Counter();
        this.commitTimeCounter = new Counter();
        this.handlerCounter = new Counter();

        init();
    }

    private static final String SEND_COUNT = "SEND_CNT";
    private static final String SEND_TIME = "SEND_TIME";
    private static final String SEND_ERROR = "SEND_ERR";
    private static final String POLL_COUNT = "POLL_CNT";
    private static final String POLL_TIME = "POLL_TIME";
    private static final String RECV_COUNT = "RECV_CNT";
    private static final String PROCESS_COUNT = "PROC_CNT";
    private static final String PROCESS_TIME = "PROC_TIME";
    private static final String PROCESS_ERROR = "PROC_ERR";
    private static final String COMMIT_COUNT = "COMMIT_CNT";
    private static final String COMMIT_TIME = "COMMIT_TIME";
    private static final String HANDLER = "HANDLER";

    private static final List<String> titles = Arrays.asList(
            SEND_COUNT,
            SEND_TIME,
            SEND_ERROR,
            POLL_COUNT,
            POLL_TIME,
            RECV_COUNT,
            PROCESS_COUNT,
            PROCESS_TIME,
            PROCESS_ERROR,
            COMMIT_COUNT,
            COMMIT_TIME,
            HANDLER
    );

    private final Map<String, Counter> titlesMap = new HashMap<>();

    private void init(){

        titlesMap.put(SEND_COUNT, sendCounter);
        titlesMap.put(SEND_TIME, sendTimeCounter);
        titlesMap.put(SEND_ERROR, sendErrorCounter);
        titlesMap.put(POLL_COUNT, pollCounter);
        titlesMap.put(POLL_TIME, pollTimeCounter);
        titlesMap.put(RECV_COUNT, recvCounter);
        titlesMap.put(PROCESS_COUNT, processCounter);
        titlesMap.put(PROCESS_TIME, processTimeCounter);
        titlesMap.put(PROCESS_ERROR, processErrorCounter);
        titlesMap.put(COMMIT_COUNT, commitCounter);
        titlesMap.put(COMMIT_TIME, commitTimeCounter);
        titlesMap.put(HANDLER, handlerCounter);
    }

    public void recordProduceSendCount(int count) {
        sendCounter.increase(count);
    }

    public void recordProduceSendTime(long time) {
        sendTimeCounter.increase(time);
    }

    public void recordProduceSendError(int count) {
        sendErrorCounter.increase(count);
    }

    public void recordConsumeRecvCount(int count) {
        recvCounter.increase(count);
    }

    public void recordConsumeProcessCount(long count) {
        processCounter.increase(count);
    }

    public void recordConsumeProcessTime(long time) {
        processTimeCounter.increase(time);
    }

    public void recordConsumeProcessErrorCount(long count) {
        processErrorCounter.increase(count);
    }

    public void recordConsumePollCount(int count) {
        pollCounter.increase(count);
    }

    public void recordConsumePollTime(long time) {
        pollTimeCounter.increase(time);
    }

    public void recordCommitCount(long count) {
        commitCounter.increase(count);
    }

    public void recordCommitTime(long time) {
        commitTimeCounter.increase(time);
    }

    public void recordConsumeHandlerCount(int count) {
        handlerCounter.increase(count);
    }

    private final AtomicLong titleCounter = new AtomicLong(-1);

    public String getStat(){
        StringBuilder builder = new StringBuilder(100);
        titleCounter.incrementAndGet();
        if(titleCounter.get() % 20 == 0){
            for(String title : titles){
                builder.append(title).append("\t");
            }
            return builder.toString();
        }
        for(String title : titles){
            int tabCount = (title.length() >>> 3) + 1;
            builder.append(titlesMap.get(title).count());
            for (int i = 0; i < tabCount; i++) {
                builder.append("\t");
            }
        }
        return builder.toString();
    }
}
