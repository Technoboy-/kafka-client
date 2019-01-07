package com.tt.kafka.consumer;

import com.tt.kafka.client.PushServerConnector;
import com.tt.kafka.consumer.exceptions.TopicNotExistException;
import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.AutoCommitMessageListener;
import com.tt.kafka.consumer.listener.BatchAcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.consumer.service.BatchAcknowledgeMessageListenerService;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.consumer.service.MessageListenerServiceRegistry;
import com.tt.kafka.metric.MonitorImpl;
import com.tt.kafka.serializer.Serializer;
import com.tt.kafka.util.CollectionUtils;
import com.tt.kafka.util.Pair;
import com.tt.kafka.util.Preconditions;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    private MessageListenerServiceRegistry serviceRegistry;

    private PushServerConnector pushServerConnector;

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

        Preconditions.checkArgument(configs.getAcknowledgeCommitBatchSize() > 0, "AcknowledgeCommitBatchSize should be greater than 0");
        Preconditions.checkArgument(configs.getBatchConsumeSize() > 0, "BatchConsumeSize should be greater than 0");


        boolean useProxy = configs.isUseProxy();

        if (start.compareAndSet(false, true)) {
            if(useProxy){
                pushServerConnector = new PushServerConnector(new Pair<MessageListener, MessageListenerService>(messageListener, messageListenerService));
                pushServerConnector.start();
            } else{
                boolean isAssignTopicPartition = !CollectionUtils.isEmpty(configs.getTopicPartitions());
                if(isAssignTopicPartition){
                    Collection<com.tt.kafka.consumer.TopicPartition> assignTopicPartitions = configs.getTopicPartitions();
                    ArrayList<TopicPartition> topicPartitions = new ArrayList<>(assignTopicPartitions.size());
                    for(com.tt.kafka.consumer.TopicPartition topicPartition : assignTopicPartitions){
                        topicPartitions.add(new TopicPartition(topicPartition.getTopic(), topicPartition.getPartition()));
                    }
                    consumer.assign(topicPartitions);
                } else{
                    if (messageListenerService instanceof ConsumerRebalanceListener) {
                        consumer.subscribe(Arrays.asList(configs.getTopic()), (ConsumerRebalanceListener) messageListenerService);
                    } else {
                        consumer.subscribe(Arrays.asList(configs.getTopic()));
                    }
                }
                //
                worker.setDaemon(true);
                worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    public void uncaughtException(Thread t, Throwable e) {
                        LOG.error("Uncaught exceptions in " + worker.getName() + ": ", e);
                    }
                });
                worker.start();
                //
            }
            Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));

            LOG.info("kafka consumer startup with info : {}", startupInfo());
        }


    }

    @Override
    public void setMessageListener(final MessageListener<K, V> messageListener) {
        if(this.messageListener != null){
            throw new IllegalArgumentException("messageListener has already set");
        }
        if (configs.isAutoCommit()
                && (messageListener instanceof AcknowledgeMessageListener)) {
            throw new IllegalArgumentException("AcknowledgeMessageListener must be mannual commit");
        }

        if (configs.isAutoCommit()
                && (messageListener instanceof BatchAcknowledgeMessageListener)) {
            throw new IllegalArgumentException("BatchAcknowledgeMessageListener must be mannual commit");
        }

        if (messageListener instanceof BatchAcknowledgeMessageListener && configs.isPartitionOrderly()) {
            throw new IllegalArgumentException("BatchAcknowledgeMessageListener not support partitionOrderly ");
        }

        if (!configs.isAutoCommit()
                && (messageListener instanceof AutoCommitMessageListener)) {
            throw new IllegalArgumentException("AutoCommitMessageListener must be auto commit");
        }

        //
        this.serviceRegistry = new MessageListenerServiceRegistry(this, messageListener);
        this.messageListenerService = this.serviceRegistry.getMessageListenerService(false);
        this.messageListener = messageListener;
    }

    public MessageListener<K, V> getMessageListener() {
        return messageListener;
    }

    @Override
    public void run() {
        LOG.info(worker.getName() + " start.");
        while (start.get()) {
            long now = System.currentTimeMillis();
            ConsumerRecords<byte[], byte[]> records = null;
            try {
                synchronized (consumer) {
                    records = consumer.poll(configs.getPollTimeout());
                }
            } catch (TopicNotExistException ex){
                StringBuilder builder = new StringBuilder(100);
                builder.append("topic not exist, will close the consumer instance in case of the scenario : ");
                builder.append("using the same groupId for subscribe more than one topic, and one of the topic does not create in the broker, ");
                builder.append("so it will cause the other one consumer in rebalance status for at least 5 minutes due to the kafka inner config.");
                builder.append("To avoid this problem, close the consumer will speed up the rebalancing time");
                LOG.error(builder.toString(), ex);
                close();
            }
            MonitorImpl.getDefault().recordConsumePollTime(System.currentTimeMillis() - now);
            MonitorImpl.getDefault().recordConsumePollCount(1);

            if (LOG.isTraceEnabled() && records != null && !records.isEmpty()) {
                LOG.trace("Received: " + records.count() + " records");
            }
            if (records != null && !records.isEmpty()) {
                invokeMessageService(records);
            } else if(records != null && messageListener instanceof BatchAcknowledgeMessageListener){
                messageListenerService.onMessage(BatchAcknowledgeMessageListenerService.EmptyConsumerRecord.EMPTY);
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

    private void invokeMessageService(ConsumerRecords<byte[], byte[]> records) {
        Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
        while (iterator.hasNext()) {
            ConsumerRecord<byte[], byte[]> record = iterator.next();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Processing " + record);
            }
            MonitorImpl.getDefault().recordConsumeRecvCount(1);

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


        return new Record(record.offset(), record.topic(), record.partition(), record.offset(),
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
            if(pushServerConnector != null){
                pushServerConnector.close();
            }
            LOG.info("KafkaConsumer closed.");
        }
    }

    /**
     * 启动信息，方便日后排查问题
     * @return
     */
    private String startupInfo(){
        boolean isAssignTopicPartition = !CollectionUtils.isEmpty(configs.getTopicPartitions());
        StringBuilder builder = new StringBuilder(200);
        builder.append("bootstrap.servers : ").append(configs.getBootstrapServers()).append(" , ");
        builder.append("group.id : ").append(configs.getGroupId()).append(" , ");
        builder.append("in ").append(isAssignTopicPartition ? "[assign] : " + configs.getTopicPartitions(): "[subscribe] : " + configs.getTopic()).append(" , ");
        builder.append("with : " + (configs.isUseProxy() ?  "proxy model " : " direct connect ")).append(" , ");
        builder.append("with listener : " + messageListener.getClass().getName()).append(" , ");
        builder.append("with listener service : " + messageListenerService.getClass().getSimpleName()).append(" ");
        return builder.toString();
    }
}
