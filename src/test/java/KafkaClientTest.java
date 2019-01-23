import com.owl.kafka.client.OwlKafkaClient;
import com.owl.kafka.client.consumer.ConsumerConfig;
import com.owl.kafka.client.consumer.KafkaConsumer;
import com.owl.kafka.client.consumer.Record;
import com.owl.kafka.client.consumer.listener.AcknowledgeMessageListener;
import com.owl.kafka.client.consumer.listener.AutoCommitMessageListener;
import com.owl.kafka.client.consumer.listener.BatchAcknowledgeMessageListener;
import com.owl.kafka.client.consumer.listener.MessageListener;
import com.owl.kafka.client.producer.Callback;
import com.owl.kafka.client.producer.KafkaProducer;
import com.owl.kafka.client.producer.ProducerConfig;
import com.owl.kafka.client.producer.SendResult;
import com.owl.kafka.client.serializer.SerializerImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Tboy
 */
public class KafkaClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientTest.class);

    @Test
    public void testSyncProducer() throws Exception {
        AtomicLong counter = new AtomicLong(1);
        ProducerConfig configs = new ProducerConfig("localhost:9092");
        configs.setKeySerializer(SerializerImpl.getFastJsonSerializer());
        configs.setValueSerializer(SerializerImpl.getFastJsonSerializer());
        KafkaProducer<String, String> producer = OwlKafkaClient.createProducer(configs);

        final AtomicBoolean alive = new AtomicBoolean(true);
        while (alive.get()) {
            String value = String.valueOf(counter.getAndIncrement());
            SendResult sendResult = producer.sendSync("test-topic", System.currentTimeMillis() + "", value);
            LOG.info("sync send value: {}, result : {}", value, sendResult);
            TimeUnit.MILLISECONDS.sleep(50);
        }

        producer.flush();
        producer.close();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            alive.set(false);
        }));
    }

    @Test
    public void testAsyncProducer() throws Exception {
        ProducerConfig configs = new ProducerConfig("localhost:9092");
        configs.setKeySerializer(SerializerImpl.getFastJsonSerializer());
        configs.setValueSerializer(SerializerImpl.getFastJsonSerializer());
        KafkaProducer<String, String> producer = OwlKafkaClient.createProducer(configs);

        final AtomicBoolean alive = new AtomicBoolean(true);
        while (alive.get()) {
            SendResult sendResult = producer.sendAsync("test-topic", null, null, "msg-" + new Date().toString(), null).get();
            LOG.info("sync send result: {}.", sendResult);
            TimeUnit.MILLISECONDS.sleep(100);
        }

        producer.flush();
        producer.close();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            alive.set(false);
        }));
    }

    @Test
    public void testAsyncCallbackProducer() throws Exception {
        ProducerConfig configs = new ProducerConfig("localhost:9092");
        configs.setKeySerializer(SerializerImpl.serializerImpl());
        configs.setValueSerializer(SerializerImpl.serializerImpl());
        KafkaProducer<String, String> producer = OwlKafkaClient.createProducer(configs);

        final AtomicBoolean alive = new AtomicBoolean(true);
        while (alive.get()) {
            producer.sendAsync("test-topic", "msg-" + new Date().toString(), new Callback() {
                @Override
                public void onCompletion(SendResult result, Exception exception) {

                    LOG.info("async send result: {}.", result);
                }
            });
            TimeUnit.MILLISECONDS.sleep(10);
        }

        producer.flush();
        producer.close();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            alive.set(false);
        }));
    }

    @Test
    public void testAutoCommitConsumer() throws Exception {
        ConsumerConfig configs = new ConsumerConfig("localhost:9092", "test-topic","test-group");
        configs.setKeySerializer(SerializerImpl.serializerImpl());
        configs.setValueSerializer(SerializerImpl.serializerImpl());
        configs.setAutoCommit(true);
        configs.put("auto.commit.interval.ms", "5000");
        configs.put("auto.offset.reset", "latest");

        MessageListener<String, String> messageListener = new AutoCommitMessageListener<String, String>() {
            @Override
            public void onMessage(Record<String, String> record) {
                String topic = record.getTopic();
                int partition = record.getPartition();
                long offset = record.getOffset();
                long timestamp = record.getTimestamp();
                String key = record.getKey();
                String value = record.getValue();

                LOG.info("partition : {}, value = {}", new Object[]{partition, value});
            }
        };
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        TimeUnit.SECONDS.sleep(100);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }

    @Test
    public void testConcurrentAutoCommitConsumer() throws Exception {
        ConsumerConfig configs = new ConsumerConfig("localhost:9092", "test-topic","test-group");
        configs.setKeySerializer(SerializerImpl.serializerImpl());
        configs.setValueSerializer(SerializerImpl.serializerImpl());
        configs.setAutoCommit(true);
        configs.put("auto.commit.interval.ms", "5000");
        configs.setParallelism(2);
        configs.put("auto.offset.reset", "latest");

        MessageListener<String, String> messageListener = new AutoCommitMessageListener<String, String>() {
            @Override
            public void onMessage(Record<String, String> record) {
                String topic = record.getTopic();
                int partition = record.getPartition();
                long offset = record.getOffset();
                long timestamp = record.getTimestamp();
                String key = record.getKey();
                String value = record.getValue();

                LOG.info(Thread.currentThread().getName() + " , partition : {}, value = {}", new Object[]{partition, value});
            }
        };
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        TimeUnit.SECONDS.sleep(100);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }

    @Test
    public void testParitionOrderlyAutoCommitConsumer() throws Exception {
        ConsumerConfig configs = new ConsumerConfig("localhost:9092", "test-topic","test-group");
        configs.setKeySerializer(SerializerImpl.serializerImpl());
        configs.setValueSerializer(SerializerImpl.serializerImpl());
        configs.setAutoCommit(true);
        configs.put("auto.commit.interval.ms", "2000");
        configs.setPartitionOrderly(true);
        configs.put("auto.offset.reset", "latest");

        MessageListener<String, String> messageListener = new AutoCommitMessageListener<String, String>() {
            @Override
            public void onMessage(Record<String, String> record) {
                String topic = record.getTopic();
                int partition = record.getPartition();
                long offset = record.getOffset();
                long timestamp = record.getTimestamp();
                String key = record.getKey();
                String value = record.getValue();

                LOG.info(Thread.currentThread().getName() + " , partition : {}, value = {}", new Object[]{partition, value});
            }
        };
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        TimeUnit.SECONDS.sleep(100);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }


    @Test
    public void testParitionOrderlyAcknowledgeConsumer() throws Exception {
        ConsumerConfig configs = new ConsumerConfig("localhost:9092", "test-topic","test-group");
        configs.setKeySerializer(SerializerImpl.getJacksonSerializer());
        configs.setValueSerializer(SerializerImpl.getJacksonSerializer());
        configs.setAutoCommit(false);
        configs.setPartitionOrderly(true);
        configs.put("auto.offset.reset", "latest");

        MessageListener<String, String> messageListener = new AcknowledgeMessageListener<String, String>() {
            @Override
            public void onMessage(Record<String, String> record, Acknowledgment acknowledgment) {
                String topic = record.getTopic();
                int partition = record.getPartition();
                long offset = record.getOffset();
                long timestamp = record.getTimestamp();
                String key = record.getKey();
                String value = record.getValue();

                LOG.info(Thread.currentThread().getName() + ", partition : {}, value = {}", new Object[]{partition, value});
                acknowledgment.acknowledge();
            }
        };
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        TimeUnit.MINUTES.sleep(100);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }

    @Test
    public void testBatchParitionOrderlyAcknowledgeConsumer() throws Exception {

        ConsumerConfig configs = new ConsumerConfig("localhost:9092", "test-topic","test-group");
        configs.setKeySerializer(SerializerImpl.getJacksonSerializer());
        configs.setValueSerializer(SerializerImpl.getJacksonSerializer());
        configs.setAutoCommit(false);
        configs.setPartitionOrderly(true);
        configs.put("auto.offset.reset", "latest");

        MessageListener<String, String> messageListener = new BatchAcknowledgeMessageListener<String, String>() {
            @Override
            public void onMessage(List<Record<String, String>> records, Acknowledgment acknowledgment) {
                for(Record<String, String> record : records){
                    String topic = record.getTopic();
                    int partition = record.getPartition();
                    long offset = record.getOffset();
                    long timestamp = record.getTimestamp();
                    String key = record.getKey();
                    String value = record.getValue();

                    LOG.info(Thread.currentThread().getName() + " , partition : {}, value = {}", new Object[]{partition, value});
                }
                acknowledgment.acknowledge();
            }
        };
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        TimeUnit.MINUTES.sleep(100);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }

    @Test
    public void testBatchAcknowledgeConsumer() throws Exception {
        ConsumerConfig configs = new ConsumerConfig("localhost:9092", "test-topic","test-group");
        configs.setKeySerializer(SerializerImpl.getJacksonSerializer());
        configs.setValueSerializer(SerializerImpl.getJacksonSerializer());
        configs.setAutoCommit(false);
        configs.put("auto.offset.reset", "latest");
        configs.setBatchConsumeTime(3);

        MessageListener<String, String> messageListener = new BatchAcknowledgeMessageListener<String, String>() {
            @Override
            public void onMessage(List<Record<String, String>> records, Acknowledgment acknowledgment) {
                for(Record<String, String> record : records){
                    String topic = record.getTopic();
                    int partition = record.getPartition();
                    long offset = record.getOffset();
                    long timestamp = record.getTimestamp();
                    String key = record.getKey();
                    String value = record.getValue();

                    LOG.info(Thread.currentThread().getName() + " , partition : {}, value: {}, offset: {}.", new Object[]{partition, value, offset});
                }

                try {
                    Thread.sleep(3000);
                } catch (Exception e) {

                }
                acknowledgment.acknowledge();
            }
        };
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        TimeUnit.MINUTES.sleep(100);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }

    @Test
    public void testAcknowledgeConsumer() throws Exception {
        ConsumerConfig configs = new ConsumerConfig("localhost:9092", "test-topic","test-group");
        configs.setKeySerializer(SerializerImpl.getJacksonSerializer());
        configs.setValueSerializer(SerializerImpl.getJacksonSerializer());
        configs.setAutoCommit(false);
        configs.put("auto.offset.reset", "latest");
        configs.setBatchConsumeTime(3);

        MessageListener<String, String> messageListener = new AcknowledgeMessageListener<String, String>() {
            @Override
            public void onMessage(Record<String, String> record, Acknowledgment acknowledgment) {
                String topic = record.getTopic();
                int partition = record.getPartition();
                long offset = record.getOffset();
                long timestamp = record.getTimestamp();
                String key = record.getKey();
                String value = record.getValue();

                LOG.info(Thread.currentThread().getName() + " , partition : {}, value: {}, offset: {}.", new Object[]{partition, value, offset});
                acknowledgment.acknowledge();
            }
        };
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        TimeUnit.MINUTES.sleep(100);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }

    @Test
    public void testProxyAcknowledgment() throws Exception{
        ConsumerConfig configs = new ConsumerConfig("localhost:9092", "test-topic","test-group");
        configs.setKeySerializer(SerializerImpl.getFastJsonSerializer());
        configs.setValueSerializer(SerializerImpl.getFastJsonSerializer());
        configs.setAutoCommit(false);
        configs.put("auto.offset.reset", "latest");
        configs.setUseProxy(true);
//        configs.setProxyModel(ConsumerConfig.ProxyModel.PUSH);
        MessageListener<String, String> messageListener = new AcknowledgeMessageListener<String, String>() {
            @Override
            public void onMessage(Record<String, String> record, Acknowledgment acknowledgment) {
                String topic = record.getTopic();
                int partition = record.getPartition();
                long offset = record.getOffset();
                long timestamp = record.getTimestamp();
                String key = record.getKey();
                String value = record.getValue();

                LOG.info("received push message : partition : {}, value: {}, offset: {}.", new Object[]{partition, value, offset});
                acknowledgment.acknowledge();
            }
        };
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        TimeUnit.MINUTES.sleep(20);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }

}
