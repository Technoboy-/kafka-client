package com.owl.kafka.client.metric;


/**
 * @Author: Tboy
 */
public interface MetricsMonitor {

    void recordProduceSendCount(int count);

    void recordProduceSendTime(long time);

    void recordProduceSendError(int count);

    void recordConsumeRecvCount(int count) ;

    void recordConsumeProcessCount(long count);

    void recordConsumeProcessTime(long time);

    void recordConsumeProcessErrorCount(long count);

    void recordConsumePollCount(int count);

    void recordConsumePollTime(long time);

    void recordCommitCount(long count);

    void recordCommitTime(long time);

    void recordConsumeHandlerCount(int count);

}
