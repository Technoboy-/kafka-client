package com.owl.kafka.client.metric;

import com.owl.kafka.client.spi.MonitorConfig;

/**
 * @Author: Tboy
 */
@MonitorConfig(value = "file", name = "kafka-stat.log", enable = true)
public interface Monitor {

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

    void stat();
}
