package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.metric.Monitor;
import com.tt.kafka.metric.MonitorImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class PartitionOrderlyAcknowledgeMessageListenerService<K, V> extends AcknowledgeMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionOrderlyAcknowledgeMessageListenerService.class);

    private final ConcurrentMap<TopicPartition, TopicPartitionHandler> handlers;

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    public PartitionOrderlyAcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer,
                                                             MessageListener<K, V> messageListener) {
        super(consumer, messageListener);
        this.consumer = consumer;
        MonitorImpl.getDefault().recordConsumeHandlerCount(-1);
        this.handlers =  new ConcurrentHashMap<>();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        super.onPartitionsAssigned(partitions);
        for (TopicPartition partition : handlers.keySet()) {
            if (!partitions.contains(partition)) {
                handlers.get(partition).stop();
                handlers.remove(partition);
                MonitorImpl.getDefault().recordConsumeHandlerCount(-1);
            }
        }
    }

    @Override
    public void onMessage(final ConsumerRecord<byte[], byte[]> record) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        TopicPartitionHandler handler = getOrCreateHandler(topicPartition);
        handler.handle(record);
    }

    @Override
    public void close() {
        for (TopicPartitionHandler handler : handlers.values()) {
            handler.stop();
        }
        super.close();
    }

    private TopicPartitionHandler getOrCreateHandler(TopicPartition topicPartition) {
        if (handlers.containsKey(topicPartition)) {
            return handlers.get(topicPartition);
        } else {
            TopicPartitionHandler handler = new TopicPartitionHandler(topicPartition.topic(), topicPartition.partition());
            TopicPartitionHandler preHandler = handlers.putIfAbsent(topicPartition, handler);
            if (preHandler != null) {
                // can not be happened.
                preHandler.stop();
            } else {
                MonitorImpl.getDefault().recordConsumeHandlerCount(1);
            }
            return handler;
        }
    }

    class TopicPartitionHandler implements Runnable {

        private Thread worker;

        private final BlockingQueue<ConsumerRecord<byte[], byte[]>> queue = new LinkedBlockingQueue<>(consumer.getConfigs().getHandlerQueueSize());

        private final AtomicBoolean start = new AtomicBoolean(false);

        public TopicPartitionHandler(String topic, int partition) {
            worker = new Thread(this, "consumer-partition-handler-" + topic + "-" + partition);
            worker.setDaemon(true);
            worker.start();
            start.compareAndSet(false, true);
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
                    PartitionOrderlyAcknowledgeMessageListenerService.super.onMessage(record);
                }
            }
        }

        public void run() {
            LOG.info("TopicPartitionHandler-{} start.", worker.getName());
            while (start.get()) {
                ConsumerRecord r = null;
                try {
                    r = queue.take();
                    PartitionOrderlyAcknowledgeMessageListenerService.super.onMessage(r);
                } catch (InterruptedException iex) {
                    LOG.error("InterruptedException onMessage ", iex);
                } catch (Throwable ex) {
                    LOG.error("onMessage error", ex);
                }
            }
            flush();
            LOG.info("TopicPartitionHandler-{} stop.", Thread.currentThread().getName());
        }
    }
}
