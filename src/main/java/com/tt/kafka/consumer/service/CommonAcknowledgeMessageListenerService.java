package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.Record;
import com.tt.kafka.metric.MonitorImpl;
import com.tt.kafka.util.NamedThreadFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
abstract class CommonAcknowledgeMessageListenerService<K, V> implements MessageListenerService<K, V>{

    private static final Logger LOG = LoggerFactory.getLogger(CommonAcknowledgeMessageListenerService.class);

    protected final ScheduledExecutorService commitScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));

    protected volatile ConcurrentMap<TopicPartition, OffsetAndMetadata> latestOffsetMap = new ConcurrentHashMap<>();

    protected final AtomicLong messageCount = new AtomicLong();

    protected final DefaultKafkaConsumerImpl<K, V> consumer;

    public CommonAcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer){
        this.consumer = consumer;
        long initialDelay = consumer.getConfigs().getAcknowledgeCommitInterval();
        long period = consumer.getConfigs().getAcknowledgeCommitInterval();
        commitScheduler.scheduleAtFixedRate(new ScheduledCommitOffsetTask(), initialDelay, period, TimeUnit.SECONDS);
    }

    protected void acknowledge(Record<K, V> record){
        if (messageCount.incrementAndGet() % consumer.getConfigs().getAcknowledgeCommitBatchSize() == 0) {
            commitScheduler.execute(new ScheduledCommitOffsetTask());
        }
        toOffsetMap(record);
    }

    protected void acknowledge(List<Record<K, V>> records){
        if (messageCount.addAndGet(records.size()) % consumer.getConfigs().getAcknowledgeCommitBatchSize() == 0) {
            commitScheduler.execute(new ScheduledCommitOffsetTask());
        }
        for(Record record : records){
            toOffsetMap(record);
        }
    }

    private void toOffsetMap(Record<K, V> record){
        TopicPartition topicPartition = new TopicPartition(record.getTopic(), record.getPartition());
        OffsetAndMetadata offsetAndMetadata = latestOffsetMap.get(topicPartition);
        if (offsetAndMetadata == null || record.getOffset() > offsetAndMetadata.offset()) {
            latestOffsetMap.put(topicPartition, new OffsetAndMetadata(record.getOffset()));
        }
    }

    class ScheduledCommitOffsetTask implements Runnable {

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            try {
                final Map<TopicPartition, OffsetAndMetadata> pre = latestOffsetMap;
                latestOffsetMap = new ConcurrentHashMap<>();
                if (pre.isEmpty()) {
                    return;
                }
                consumer.commit(pre);
            } catch (Throwable ex) {
                LOG.error("Commit consumer offset error.", ex);
            } finally {
                MonitorImpl.getDefault().recordCommitCount(1L);
                MonitorImpl.getDefault().recordCommitTime(System.currentTimeMillis() - now);
            }
        }
    }

    public void close(){
        commitScheduler.shutdown();
        if (!latestOffsetMap.isEmpty()) {
            new ScheduledCommitOffsetTask().run();
        }
    }

}
