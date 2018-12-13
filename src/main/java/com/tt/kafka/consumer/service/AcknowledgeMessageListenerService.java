package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.metric.Monitor;
import com.tt.kafka.util.NamedThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
public class AcknowledgeMessageListenerService<K, V> extends ReblanceMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgeMessageListenerService.class);

    private volatile ConcurrentMap<TopicPartition, OffsetAndMetadata> latestOffsetMap = new ConcurrentHashMap<>();

    private final AtomicLong messageCount = new AtomicLong();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private final AcknowledgeMessageListener<K, V> messageListener;

    public AcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, AcknowledgeMessageListener<K, V> messageListener) {
        this.consumer = consumer;
        this.messageListener = messageListener;
        long initialDelay = consumer.getConfigs().getAcknowledgeCommitInterval();
        long period = consumer.getConfigs().getAcknowledgeCommitInterval();
        scheduler.scheduleAtFixedRate(new ScheduledCommitOffsetTask(), initialDelay, period, TimeUnit.SECONDS);
        Monitor.getInstance().recordConsumeHandlerCount(1);
    }

    @Override
    public void onMessage(final ConsumerRecord<byte[], byte[]> record) {
        long now = System.currentTimeMillis();
        try {
            messageListener.onMessage(consumer.toRecord(record), new AcknowledgeMessageListener.Acknowledgment() {
                @Override
                public void acknowledge() {
                    if (messageCount.getAndIncrement() % consumer.getConfigs().getAcknowledgeCommitBatchSize() == 0) {
                        scheduler.execute(new ScheduledCommitOffsetTask());
                    } else {
                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        OffsetAndMetadata offsetAndMetadata = latestOffsetMap.get(topicPartition);
                        if (offsetAndMetadata == null || record.offset() > offsetAndMetadata.offset()) {
                            latestOffsetMap.put(topicPartition, new OffsetAndMetadata(record.offset()));
                        }
                    }
                }
            });
        } catch (Throwable ex) {
            Monitor.getInstance().recordConsumeProcessErrorCount(1);
            LOG.error("onMessage error", ex);
        } finally {
            Monitor.getInstance().recordConsumeProcessCount(1);
            Monitor.getInstance().recordConsumeProcessTime(System.currentTimeMillis() - now);
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        if (!latestOffsetMap.isEmpty()) {
            new ScheduledCommitOffsetTask().run();
        }
        LOG.debug("AcknowledgeMessageListenerService stop.");
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
                Monitor.getInstance().recordCommitCount(1L);
                Monitor.getInstance().recordCommitTime(System.currentTimeMillis() - now);
            }
        }
    }
}
