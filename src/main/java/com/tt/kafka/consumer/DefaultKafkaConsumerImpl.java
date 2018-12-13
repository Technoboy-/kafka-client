package com.tt.kafka.consumer;

import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.AutoCommitMessageListener;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.consumer.service.*;
import com.tt.kafka.serializer.Serializer;
import com.tt.kafka.util.Preconditions;
import com.tt.kafka.metric.Monitor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
@SuppressWarnings("all")
public class DefaultKafkaConsumerImpl<K, V> implements Runnable, KafkaConsumer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultKafkaConsumerImpl.class);

    private final AtomicBoolean start = new AtomicBoolean(false);

    private Consumer<byte[], byte[]> consumer;

    private MessageListener<K, V> messageListener;

    private final Thread worker = new Thread(this, "consumer-poll-worker");

    private final ConsumerConfig configs;

    private MessageListenerService messageListenerService;

    private Serializer keySerializer;

    private Serializer valueSerializer;

    public DefaultKafkaConsumerImpl(ConsumerConfig configs) {
        this.configs = configs;
        keySerializer = configs.getKeySerializer();
        valueSerializer = configs.getValueSerializer();

        // KAFKA 0.11 later version.
        if(configs.get("partition.assignment.strategy") == null){
            configs.put("partition.assignment.strategy", "com.tt.kafka.consumer.assignor.CheckTopicStickyAssignor");
        }
        configs.put("bootstrap.servers", configs.getBootstrapServers());
        configs.put("group.id", configs.getGroupId());

        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(configs, new ByteArrayDeserializer(), new ByteArrayDeserializer());

    }

    @Override
    public void start() {
        Preconditions.checkArgument(keySerializer != null , "keySerializer should not be null");
        Preconditions.checkArgument(valueSerializer != null , "valueSerializer should not be null");
        Preconditions.checkArgument(messageListener != null , "messageListener should not be null");
        Preconditions.checkArgument(messageListenerService != null, "messageListener implementation error");

        if (start.compareAndSet(false, true)) {
            if (messageListenerService instanceof ConsumerRebalanceListener) {
                consumer.subscribe(Arrays.asList(configs.getTopic()), (ConsumerRebalanceListener) messageListenerService);
            } else {
                consumer.subscribe(Arrays.asList(configs.getTopic()));
            }
            worker.setDaemon(true);
            worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Uncaught exceptions in " + worker.getName() + ": ", e);
                }
            });
            worker.start();
            //
            Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
        }
    }

    @Override
    public void setMessageListener(final MessageListener<K, V> messageListener) {
        if (configs.isAutoCommit()
                && (messageListener instanceof AcknowledgeMessageListener)) {
            throw new IllegalArgumentException("AcknowledgeMessageListener must be mannual commit");
        }

        if (!configs.isAutoCommit()
                && (messageListener instanceof AutoCommitMessageListener)) {
            throw new IllegalArgumentException("AutoCommitMessageListener must be auto commit");
        }

        //
        boolean partitionOrderly = configs.isPartitionOrderly();

        if (messageListener instanceof AcknowledgeMessageListener) {
            AcknowledgeMessageListener acknowledgeMessageListener = (AcknowledgeMessageListener) messageListener;
            if (partitionOrderly) {
                messageListenerService = new PartitionOrderlyAcknowledgeMessageListenerService(this, acknowledgeMessageListener);
            } else {
                messageListenerService = new AcknowledgeMessageListenerService(this, acknowledgeMessageListener);
            }
        } else if (messageListener instanceof AutoCommitMessageListener) {
            AutoCommitMessageListener autoCommitMessageListener = (AutoCommitMessageListener) messageListener;
            int parallel = configs.getConcurrentNum();
            if(partitionOrderly){
                messageListenerService = new PartitionOrderlyAutoCommitMessageService(this, autoCommitMessageListener);
            } else if(parallel <= 0){
                messageListenerService = new AutoCommitMessageListenerService(this, autoCommitMessageListener);
            } else{
                messageListenerService = new ConcurrentAutoCommitMessageListenerService(parallel, this, autoCommitMessageListener);
            }
        }
        this.messageListener = messageListener;
    }

    public MessageListener<K, V> getMessageListener() {
        return messageListener;
    }

    @Override
    public void run() {
        LOG.info(worker.getName() + " start.");
        LOG.info("starting subscribe topic: {}, group: {} ", configs.getTopic(), configs.getGroupId());
        while (start.get()) {
            long now = System.currentTimeMillis();
            ConsumerRecords<byte[], byte[]> records;
            synchronized (consumer) {
                records = consumer.poll(configs.getPollTimeout());
            }
            Monitor.getInstance().recordConsumePollTime(System.currentTimeMillis() - now);
            Monitor.getInstance().recordConsumePollCount(1);

            if (LOG.isTraceEnabled() && records != null && !records.isEmpty()) {
                LOG.trace("Received: " + records.count() + " records");
            }
            if (records != null && !records.isEmpty()) {
                invokeMessageHandler(records);
            }
        }
        LOG.info(worker.getName() + " stop.");
    }

    public void commit(Map<TopicPartition, OffsetAndMetadata> highestOffsetRecords) {
        synchronized (consumer) {
            consumer.commitAsync(highestOffsetRecords, new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if(exception != null){
                        LOG.warn("commit async fail, metadata {}, exceptions {}", offsets, exception);
                    }
                }
            });
            if(LOG.isDebugEnabled()){
                LOG.debug("commit offset : {}", highestOffsetRecords);
            }
        }
    }

    private void invokeMessageHandler(ConsumerRecords<byte[], byte[]> records) {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
        while (iterator.hasNext()) {
            ConsumerRecord<byte[], byte[]> record = iterator.next();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Processing " + record);
            }
            Monitor.getInstance().recordConsumeRecvCount(1);

            try {
                messageListenerService.onMessage(record);
            } catch (Throwable ex) {
                LOG.error("onMessage error", ex);
            }
        }
    }

    public ConsumerConfig getConfigs() {
        return configs;
    }

    public Record toRecord(ConsumerRecord<byte[], byte[]> record) {
        byte[] keyBytes = record.key();
        byte[] valueBytes = record.value();


        return new Record(record.topic(), record.partition(), record.offset(),
                keyBytes != null ? (K) keySerializer.deserialize(record.key(), Object.class) : null,
                valueBytes != null ? (V) valueSerializer.deserialize(record.value(), Object.class) : null,
                record.timestamp());
    }

    @Override
    public void close() {
        LOG.info("KafkaConsumer closing.");
        if(start.compareAndSet(true, false)){
            synchronized (consumer) {
                if (messageListenerService != null) {
                    messageListenerService.close();
                }
                consumer.unsubscribe();
                consumer.close();
            }
            LOG.info("KafkaConsumer closed.");
        }
    }
}
