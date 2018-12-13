import com.tt.kafka.TTKafkaClient;
import com.tt.kafka.consumer.ConsumerConfig;
import com.tt.kafka.consumer.KafkaConsumer;
import com.tt.kafka.consumer.Record;
import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.AutoCommitMessageListener;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.producer.Callback;
import com.tt.kafka.producer.KafkaProducer;
import com.tt.kafka.producer.ProducerConfig;
import com.tt.kafka.producer.SendResult;
import com.tt.kafka.serializer.SerializerImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * create by WangTao on 2018/11/23
 */
public class KafkaClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientTest.class);

    @Test
    public void testSyncProducer() throws Exception {
        ProducerConfig configs = new ProducerConfig("localhost:9092");
        configs.setKeySerializer(SerializerImpl.getJacksonSerializaer());
        configs.setValueSerializer(SerializerImpl.getJacksonSerializaer());
        KafkaProducer<String, String> producer = TTKafkaClient.createProducer(configs);

        final AtomicBoolean alive = new AtomicBoolean(true);
        while (alive.get()) {
            SendResult sendResult = producer.sendSync("test-topic", System.currentTimeMillis() + "", "msg-" + new Date().toString());
            LOG.info("traceId: {}, sync send result: {}.", "", sendResult);
            TimeUnit.MILLISECONDS.sleep(10);
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
        configs.setKeySerializer(SerializerImpl.serializerImpl());
        configs.setValueSerializer(SerializerImpl.serializerImpl());
        KafkaProducer<String, String> producer = TTKafkaClient.createProducer(configs);

        final AtomicBoolean alive = new AtomicBoolean(true);
        while (alive.get()) {
            SendResult sendResult = producer.sendAsync("test-topic", null, null, "msg-" + new Date().toString(), null).get();
            LOG.info("traceId: {}, sync send result: {}.", "", sendResult);
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
        KafkaProducer<String, String> producer = TTKafkaClient.createProducer(configs);

        final AtomicBoolean alive = new AtomicBoolean(true);
        while (alive.get()) {
            producer.sendAsync("test-topic", "msg-" + new Date().toString(), new Callback() {
                @Override
                public void onCompletion(SendResult result, Exception exception) {

                    LOG.info("traceId: {}, async send result: {}.", "", result);
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

                LOG.info("traceId: {}, partition : {}, value = {}", new Object[]{"", partition, value});
            }
        };
        KafkaConsumer<String, String> consumer = TTKafkaClient.createConsumer(configs);
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
        configs.setKeySerializer(SerializerImpl.getJacksonSerializaer());
        configs.setValueSerializer(SerializerImpl.getJacksonSerializaer());
        configs.setAutoCommit(true);
        configs.put("auto.commit.interval.ms", "5000");
        configs.setConcurrentNum(2);
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

                LOG.info(Thread.currentThread().getName() + " traceId: {}, partition : {}, value = {}", new Object[]{"", partition, value});
            }
        };
        KafkaConsumer<String, String> consumer = TTKafkaClient.createConsumer(configs);
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

                LOG.info(Thread.currentThread().getName() + " traceId: {} , partition : {}, value = {}", new Object[]{"", partition, value});
            }
        };
        KafkaConsumer<String, String> consumer = TTKafkaClient.createConsumer(configs);
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
        configs.setKeySerializer(SerializerImpl.serializerImpl());
        configs.setValueSerializer(SerializerImpl.serializerImpl());
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

                LOG.info(Thread.currentThread().getName() + " traceId: {} , partition : {}, value = {}", new Object[]{"", partition, value});
                acknowledgment.acknowledge();
            }
        };
        KafkaConsumer<String, String> consumer = TTKafkaClient.createConsumer(configs);
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
        configs.setKeySerializer(SerializerImpl.serializerImpl());
        configs.setValueSerializer(SerializerImpl.serializerImpl());
        configs.setAutoCommit(false);
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

                LOG.info(Thread.currentThread().getName() + " traceId: {} , partition : {}, value: {}, offset: {}.", new Object[]{"", partition, value, offset});
                acknowledgment.acknowledge();
            }
        };
        KafkaConsumer<String, String> consumer = TTKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        TimeUnit.MINUTES.sleep(100);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }
}
