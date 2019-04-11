package com.owl.kafka.client.metric;



import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Author: Tboy
 */
public class ProducerFileMetricsMonitor extends FileMetricsMonitor{

    public ProducerFileMetricsMonitor(String name) {
        super(name);
    }

    @Override
    public IMonitor getIMonitor() {
        return new ProducerrMonitor();
    }

    private class ProducerrMonitor implements IMonitor {

        @Override
        public void configure(Map<String, String> monitor) {
            monitor.put(SEND_COUNT, sendCounter.getCount() + "");
            monitor.put(SEND_TIME, sendTimeCounter.getCount() + "");
            monitor.put(SEND_ERROR, sendErrorCounter.getCount() + "");

        }

        private final List<String> statTitles = Arrays.asList(
                SEND_COUNT,
                SEND_TIME,
                SEND_ERROR
        );

        @Override
        public List<String> getStatTitles() {
            return statTitles;
        }

        @Override
        public long getMonitorInterval() {
            return defaultIntervalTime;
        }

    }

}
