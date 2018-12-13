package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.listener.AutoCommitMessageListener;
import com.tt.kafka.metric.Monitor;
import com.tt.kafka.util.CallerWaitPolicy;
import com.tt.kafka.util.NamedThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @Author: Tboy
 */
public class ConcurrentAutoCommitMessageListenerService<K, V> extends ReblanceMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentAutoCommitMessageListenerService.class);

    private final ThreadPoolExecutor executor;

    private final AutoCommitMessageListener<K, V> messageListener;

    private final DefaultKafkaConsumerImpl<K, V> consumer;

    public ConcurrentAutoCommitMessageListenerService(int num, DefaultKafkaConsumerImpl<K, V> consumer, AutoCommitMessageListener<K, V> listener) {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(consumer.getConfigs().getHandlerQueueSize());
        executor = new ThreadPoolExecutor(num, num, 1, TimeUnit.MINUTES, queue,
                new NamedThreadFactory("concurrent-consumer-worker"), new CallerWaitPolicy(queue));
        this.consumer = consumer;
        this.messageListener = listener;
        Monitor.getInstance().recordConsumeHandlerCount(num);
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
                Monitor.getInstance().recordConsumeProcessErrorCount(1);
                LOG.error("onMessage error", ex);
            } finally {
                Monitor.getInstance().recordConsumeProcessCount(1);
                Monitor.getInstance().recordConsumeProcessTime(System.currentTimeMillis() - now);
            }
        }
    }
}
