package com.tt.kafka.producer;

import com.tt.kafka.metric.Monitor;
import com.tt.kafka.metric.MonitorImpl;
import com.tt.kafka.serializer.Serializer;
import com.tt.kafka.util.Preconditions;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class DefaultKafkaProducerImpl<K, V> implements KafkaProducer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultKafkaProducerImpl.class);

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final ProducerConfig configs;

    private final Producer<byte[], byte[]> producer;

    private Serializer keySerializer;

    private Serializer valueSerializer;

    public DefaultKafkaProducerImpl(ProducerConfig configs) {
        this.configs = configs;
        configs.put("bootstrap.servers", configs.getBootstrapServers());
        configs.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        keySerializer = configs.getKeySerializer();
        valueSerializer = configs.getValueSerializer();

        Preconditions.checkArgument(keySerializer != null , "keySerializer should not be null");
        Preconditions.checkArgument(valueSerializer != null , "valueSerializer should not be null");

        this.producer = new org.apache.kafka.clients.producer.KafkaProducer(configs);
        start.compareAndSet(false, true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
    }

    public SendResult sendSync(String topic, V value) {
        return sendSync(topic, null, value);
    }

    public SendResult sendSync(String topic, K key, V value) {
        return sendSync(topic, null, key, value);
    }

    public SendResult sendSync(String topic, Integer partition, K key, V value) {
        long now = System.currentTimeMillis();
        try {
            return doSend(topic, partition, key, value, null).get();
        } catch (Exception ex) {
            MonitorImpl.getDefault().recordProduceSendError(1);
            throw new RuntimeException(ex);
        } finally {
            MonitorImpl.getDefault().recordProduceSendCount(1);
            MonitorImpl.getDefault().recordProduceSendTime(System.currentTimeMillis() - now);
        }
    }

    public Future<SendResult> sendAsync(String topic, V value, Callback callback) {
        return sendAsync(topic,  null, value, callback);
    }

    public Future<SendResult> sendAsync(String topic, K key, V value, Callback callback) {
        return sendAsync(topic, null, key, value, callback);
    }

    public Future<SendResult> sendAsync(String topic, Integer partition, K key, V value, Callback callback) {
        long now = System.currentTimeMillis();
        try {
            return doSend(topic, partition, key, value, callback);
        } catch (Exception ex) {
            MonitorImpl.getDefault().recordProduceSendError(1);
            throw new RuntimeException(ex);
        } finally {
            MonitorImpl.getDefault().recordProduceSendCount(1);
            MonitorImpl.getDefault().recordProduceSendTime(System.currentTimeMillis() - now);
        }
    }

    private Future<SendResult> doSend(String topic, Integer partition, K key, V value, Callback callback) {
        if(!start.get()){
            throw new RuntimeException("kafka producer has closed !");
        }

        byte[] keyBytes = keySerializer.serialize(key);
        byte[] valueBytes = valueSerializer.serialize(value);

        ProducerRecord<byte[], byte[]> record;

        // mark partition invalid.
        if(partition == null) {
            partition = -1;
        }
        if (partition == -1 && null == key) {
            record = new ProducerRecord<>(topic, valueBytes);
        } else if (partition == -1) {
            record = new ProducerRecord<>(topic, keyBytes, valueBytes);
        } else {
            record = new ProducerRecord<>(topic, partition, keyBytes, valueBytes);
        }

        final Future<RecordMetadata> future = producer.send(record, new org.apache.kafka.clients.producer.Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (callback != null) {
                    callback.onCompletion(new SendResult(metadata.partition(), metadata.offset()), exception);
                }
            }
        });

        return new Future<SendResult>(){
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return future.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return future.isCancelled();
            }

            @Override
            public boolean isDone() {
                return future.isDone();
            }

            @Override
            public SendResult get() throws InterruptedException, ExecutionException {
                RecordMetadata recordMetadata = future.get();
                return new SendResult(recordMetadata.partition(), recordMetadata.offset());
            }

            @Override
            public SendResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                RecordMetadata recordMetadata = future.get(timeout, unit);
                return new SendResult(recordMetadata.partition(), recordMetadata.offset());
            }
        };
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        LOG.info("KafkaProducer closing.");
        if(start.compareAndSet(true, false)){
            flush();
            producer.close(timeout, unit);
            LOG.info("KafkaProducer closed.");
        }
    }
}
