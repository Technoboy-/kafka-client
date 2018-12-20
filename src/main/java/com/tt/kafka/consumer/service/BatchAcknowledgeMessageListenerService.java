package com.tt.kafka.consumer.service;


import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.Record;
import com.tt.kafka.consumer.listener.BatchAcknowledgeMessageListener;
import com.tt.kafka.metric.Monitor;
import com.tt.kafka.metric.MonitorImpl;
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
public class BatchAcknowledgeMessageListenerService<K, V> extends BaseAcknowledgeMessageListenerService<K, V>{

    private static final Logger LOG = LoggerFactory.getLogger(BatchAcknowledgeMessageListenerService.class);

    private final BatchAcknowledgeMessageListener<K, V> messageListener;

    private List<Record<K, V>> container;

    private final int batchConsumeSize;

    private final long batchConsumeTime;

    private long lastConsumeTime = System.currentTimeMillis();

    private final Semaphore semaphore = new Semaphore(1);

    public BatchAcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, BatchAcknowledgeMessageListener<K, V> messageListener) {
        super(consumer);
        this.messageListener = messageListener;
        this.batchConsumeSize = consumer.getConfigs().getBatchConsumeSize();
        this.batchConsumeTime = TimeUnit.SECONDS.toMillis(consumer.getConfigs().getBatchConsumeTime());
        this.container = new ArrayList<>(this.batchConsumeSize);
        MonitorImpl.getDefault().recordConsumeHandlerCount(1);
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
            messageListener.onMessage(records, new BatchAcknowledgeMessageListener.Acknowledgment() {
                @Override
                public void acknowledge() {
                    BatchAcknowledgeMessageListenerService.super.acknowledge(records);
                    semaphore.release();
                }
            });
            lastConsumeTime = System.currentTimeMillis();
        } catch (Throwable ex) {
            semaphore.release();
            MonitorImpl.getDefault().recordConsumeProcessErrorCount(this.batchConsumeSize);
            LOG.error("onMessage error", ex);
        } finally {
            MonitorImpl.getDefault().recordConsumeProcessCount(this.batchConsumeSize);
            MonitorImpl.getDefault().recordConsumeProcessTime(System.currentTimeMillis() - now);
        }
    }

    @Override
    public void close() {
        super.close();
        LOG.debug("BatchAcknowledgeMessageListenerService stop.");
    }
}
