package com.owl.kafka.client.consumer.service;

import com.owl.kafka.client.consumer.listener.AutoCommitMessageListener;
import com.owl.kafka.client.consumer.listener.MessageListener;
import com.owl.kafka.client.util.CallerWaitPolicy;
import com.owl.kafka.client.util.NamedThreadFactory;
import com.owl.kafka.client.consumer.DefaultKafkaConsumerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public class ConcurrentAutoCommitMessageListenerService<K, V> extends RebalanceMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentAutoCommitMessageListenerService.class);

    private final ThreadPoolExecutor executor;

    private final AutoCommitMessageListener<K, V> messageListener;

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    public ConcurrentAutoCommitMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, MessageListener<K, V> listener) {
        int parallelism = consumer.getConfigs().getParallelism();
        executor = new ThreadPoolExecutor(parallelism, parallelism, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(consumer.getConfigs().getHandlerQueueSize()),
                new NamedThreadFactory("concurrent-consumer-worker"), new CallerWaitPolicy());
        this.consumer = consumer;
        this.messageListener = (AutoCommitMessageListener)listener;
        consumer.getMetricsMonitor().recordConsumeHandlerCount(parallelism);
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        executor.execute(new Task(record));
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    class Task implements Runnable {

        private final ConsumerRecord<byte[], byte[]> record;

        public Task(ConsumerRecord<byte[], byte[]> record) {
            this.record = record;
        }

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            try {
                messageListener.onMessage(consumer.toRecord(record));
            } catch (Throwable ex) {
                consumer.getMetricsMonitor().recordConsumeProcessErrorCount(1);
                LOG.error("onMessage error", ex);
            } finally {
                consumer.getMetricsMonitor().recordConsumeProcessCount(1);
                consumer.getMetricsMonitor().recordConsumeProcessTime(System.currentTimeMillis() - now);
            }
        }
    }
}
