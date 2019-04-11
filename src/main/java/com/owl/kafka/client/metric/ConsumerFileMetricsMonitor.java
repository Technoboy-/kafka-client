package com.owl.kafka.client.metric;



import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Author: Tboy
 */
public class ConsumerFileMetricsMonitor extends FileMetricsMonitor{

    public ConsumerFileMetricsMonitor(String name) {
        super(name);
    }

    @Override
    public IMonitor getIMonitor() {
        return new ConsumerMonitor();
    }

    private class ConsumerMonitor implements IMonitor {

        @Override
        public void configure(Map<String, String> monitor) {
            monitor.put(POLL_COUNT, pollCounter.getCount() + "");
            monitor.put(POLL_TIME, pollTimeCounter.getCount() + "");
            monitor.put(RECV_COUNT, recvCounter.getCount() + "");
            monitor.put(PROCESS_COUNT, processCounter.getCount() + "");
            monitor.put(PROCESS_TIME, processTimeCounter.getCount() + "");
            monitor.put(PROCESS_ERROR, processErrorCounter.getCount() + "");
            monitor.put(COMMIT_COUNT, commitCounter.getCount() + "");
            monitor.put(COMMIT_TIME, commitTimeCounter.getCount() + "");
            monitor.put(HANDLER, handlerCounter.get() + "");
        }

        private final List<String> statTitles = Arrays.asList(
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
