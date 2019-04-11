package com.owl.kafka.client.consumer.service;

import com.owl.kafka.client.consumer.listener.AutoCommitMessageListener;
import com.owl.kafka.client.consumer.listener.MessageListener;
import com.owl.kafka.client.consumer.DefaultKafkaConsumerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class PartitionOrderlyAutoCommitMessageListenerService<K, V> extends RebalanceMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionOrderlyAutoCommitMessageListenerService.class);

    private final ConcurrentMap<TopicPartition, TopicPartitionHandler<K, V>> handlers;

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    private final AutoCommitMessageListener<K, V> messageListener;

    public PartitionOrderlyAutoCommitMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, MessageListener<K, V> messageListener) {
        this.handlers = new ConcurrentHashMap<>();
        this.consumer = consumer;
        this.messageListener = (AutoCommitMessageListener)messageListener;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        super.onPartitionsAssigned(partitions);
        for (TopicPartition partition : handlers.keySet()) {
            if (!partitions.contains(partition)) {
                handlers.get(partition).stop();
                handlers.remove(partition);
                consumer.getMetricsMonitor().recordConsumeHandlerCount(-1);
            }
        }
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        TopicPartitionHandler<K, V> handler = getOrCreateHandler(topicPartition);
        handler.handle(record);
    }

    private TopicPartitionHandler<K, V> getOrCreateHandler(TopicPartition topicPartition) {
        if (handlers.containsKey(topicPartition)) {
            return handlers.get(topicPartition);
        } else {
            TopicPartitionHandler<K, V> handler = new TopicPartitionHandler<>(topicPartition.topic(), topicPartition.partition());
            TopicPartitionHandler<K, V> preHandler = handlers.putIfAbsent(topicPartition, handler);
            if (preHandler != null) {
                // can not be happened.
                preHandler.stop();
            } else {
                consumer.getMetricsMonitor().recordConsumeHandlerCount(1);
            }
            return handler;
        }
    }

    @Override
    public void close() {
        for (TopicPartitionHandler handler : handlers.values()) {
            handler.stop();
        }
    }

    class TopicPartitionHandler<K, V> implements Runnable {

        private Thread worker;

        private final ArrayBlockingQueue<ConsumerRecord<byte[], byte[]>> queue = new ArrayBlockingQueue<>(consumer.getConfigs().getHandlerQueueSize());

        private final AtomicBoolean start = new AtomicBoolean(false);

        public TopicPartitionHandler(String topic, int partition) {
            worker = new Thread(this, "consumer-partition-handler-" + topic + "-" + partition);
            start.compareAndSet(false, true);
            worker.setDaemon(true);
            worker.start();
        }

        public void stop() {
            start.compareAndSet(true, false);
            worker.interrupt();
        }

        public void handle(final ConsumerRecord<byte[], byte[]> record) {
            try {
                queue.put(record);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void flush() {
            if (queue.size() > 0) {
                List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>(queue.size());
                queue.drainTo(records);
                for (ConsumerRecord record : records) {
                    messageListener.onMessage(consumer.toRecord(record));
                }
            }
        }

        public void run() {
            LOG.info("TopicPartitionHandler-{} start.", worker.getName());
            while (start.get()) {
                long now = 0;
                ConsumerRecord r = null;
                try {
                    r = queue.take();
                    now = System.currentTimeMillis();
                    messageListener.onMessage(consumer.toRecord(r));
                } catch (InterruptedException iex) {
                    LOG.error("InterruptedException onMessage ", iex);
                } catch (Throwable ex) {
                    consumer.getMetricsMonitor().recordConsumeProcessErrorCount(1);
                    LOG.error("onMessage error", ex);
                } finally {
                    if (now > 0) {
                        consumer.getMetricsMonitor().recordConsumeProcessCount(1);
                        consumer.getMetricsMonitor().recordConsumeProcessTime(System.currentTimeMillis() - now);
                    }
                }
            }
            flush();
            LOG.info("TopicPartitionHandler-{} stop.", worker.getName());
        }
    }
}
