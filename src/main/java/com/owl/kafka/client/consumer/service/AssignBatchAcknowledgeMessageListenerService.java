package com.owl.kafka.client.consumer.service;


import com.owl.kafka.client.consumer.listener.BatchAcknowledgeMessageListener;
import com.owl.kafka.client.consumer.listener.MessageListener;
import com.owl.kafka.client.consumer.DefaultKafkaConsumerImpl;
import com.owl.kafka.client.consumer.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class AssignBatchAcknowledgeMessageListenerService<K, V> extends CommonAcknowledgeMessageListenerService<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(AssignBatchAcknowledgeMessageListenerService.class);

    private final BatchAcknowledgeMessageListener<K, V> messageListener;

    private List<Record<K, V>> container;

    private final int batchConsumeSize;

    private final long batchConsumeTime;

    private long lastConsumeTime = System.currentTimeMillis();

    private final Semaphore semaphore = new Semaphore(1);

    public AssignBatchAcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, MessageListener<K, V> messageListener) {
        super(consumer);
        this.messageListener = (BatchAcknowledgeMessageListener)messageListener;
        this.batchConsumeSize = consumer.getConfigs().getBatchConsumeSize();
        this.batchConsumeTime = TimeUnit.SECONDS.toMillis(consumer.getConfigs().getBatchConsumeTime());
        this.container = new ArrayList<>(this.batchConsumeSize);
        consumer.getMetricsMonitor().recordConsumeHandlerCount(1);
    }

    @Override
    public void onMessage(final ConsumerRecord<byte[], byte[]> record) {
        long now = System.currentTimeMillis();
        try {
            container.add(consumer.toRecord(record));
            if(container.size() < this.batchConsumeSize || System.currentTimeMillis() + batchConsumeTime < lastConsumeTime){
                return;
            }
            semaphore.acquire();
            final List<Record<K, V>> records = this.container;
            this.container = new ArrayList<>(this.batchConsumeSize);
            this.messageListener.onMessage(records, new BatchAcknowledgeMessageListener.Acknowledgment() {
                @Override
                public void acknowledge() {
                    AssignBatchAcknowledgeMessageListenerService.super.acknowledge(records);
                    semaphore.release();
                }
            });
            lastConsumeTime = System.currentTimeMillis();
        } catch (Throwable ex) {
            semaphore.release();
            consumer.getMetricsMonitor().recordConsumeProcessErrorCount(this.batchConsumeSize);
            LOG.error("onMessage error", ex);
        } finally {
            consumer.getMetricsMonitor().recordConsumeProcessCount(1);
            consumer.getMetricsMonitor().recordConsumeProcessTime(System.currentTimeMillis() - now);
        }
    }

    @Override
    public void close() {
        super.close();
        LOG.debug("AssignBatchAcknowledgeMessageListenerService stop.");
        consumer.getMetricsMonitor().recordConsumeHandlerCount(-1);
    }
}
