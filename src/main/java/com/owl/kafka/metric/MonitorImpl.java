package com.owl.kafka.metric;

import com.owl.kafka.common.spi.MonitorLoader;
import com.owl.kafka.common.spi.MonitorConfig;

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
