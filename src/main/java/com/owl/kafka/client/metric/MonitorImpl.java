package com.owl.kafka.client.metric;

import com.owl.kafka.client.spi.MonitorLoader;
import com.owl.kafka.client.spi.MonitorConfig;

/**
 * @Author: Tboy
 */
public class MonitorImpl{

    public static Monitor getDefault() {
        return MonitorLoader.getSPIClass(Monitor.class, MonitorConfig.class).getExtension();
    }

    public static Monitor getFileMonitor() {
        return MonitorLoader.getSPIClass(Monitor.class, MonitorConfig.class).getExtension("file");
    }

}
