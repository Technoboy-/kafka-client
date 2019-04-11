package com.owl.kafka.client.metric;

import java.util.List;
import java.util.Map;

/**
 * @Author: Tboy
 */
public interface IMonitor {

    long defaultIntervalTime = 1000L;

    void configure(Map<String,String> map);

    List<String> getStatTitles();

    long getMonitorInterval();
}
