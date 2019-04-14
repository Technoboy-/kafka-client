#### 一. 版本
目前Kafka client SDK基于官方kafka-clients 0.11.0.3开发，要求broker版本为0.11及之后，低于该版本不予以支持。

#### 二. 依赖
自行下载源码，进行mvn install或通过mvn deploy私服。

#### 三. Producer
对于生产者，提供了同步发送，异步发送。
```java
public class ProducerExample {
    public static void main(String[] args) throws Exception {

        // Kafka集群地址
        String boostrapServers = "localhost:9092";
        
        ProducerConfig configs = new ProducerConfig(boostrapServers);

        // key 序列化器，必须要传入
        configs.setKeySerializer(SerializerImpl.getStringSerializer());

        // value 序列化器，必须要传入
        configs.setValueSerializer(SerializerImpl.getStringSerializer());

        // 构造 Producer
        final KafkaProducer<String, String> producer = OwlKafkaClient.createProducer(configs);

        // 同步发送
        SendResult syncSendResult = producer.sendSync("test-topic", "test-key","test-msg");

        // 异步发送，无Callback
        Future<SendResult> asyncSendResultFuture = producer.sendAsync("test-topic", "test-key", "test-msg", null);

        // 异步发送，提供Callback
        producer.sendAsync("test-topic", "test-key", "test-msg", new Callback() {
            @Override
            public void onCompletion(SendResult result, Exception exception) {
                if (exception != null) {
                    // log exception.
                } else {
                    // do something.
                }
            }
        });

        // 强制flush，会将缓存区所有未发送的message强制发送出去
        producer.flush();

        // close 程序退出是主动调用关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
        }));
    }
}
```
**参数说明**
```
//对于常用的属性，进行了封装:
configs.setAcks("1");
configs.setRetries(1);
configs.setBatchSize(16384);
//对于不常用的属性，通过put方式设置:
configs.put("compression.type", "none");
```

#### 四. Consumer
对于消费者，提供自动提交offset和手动提交offset。对于自动提交offset，通过参数可以实现单线程消费自动提交offset，多线程消费自动提交offset，分区有序消费自动提交offset。对于手动提交，通过参数，可以实现单线程消费手动提交offset，分区有序手动提交offset。两种方式，都通过设置listener模式回调业务。
```java
//自动提交offset
public class AutoCommitConsumerExample {

    public static void main(String[] args) {

        // Kafka集群地址
        String boostrapServers = "localhost:9092";

        // 订阅topic，只支持单个topic。当指定topic后，内部为subscribe模式，支持rebalance操作。
        String topic = "test-topic";
        
        // 订阅topic和分区。当指定TopicPartition后，内部为assign模式，不支持rebalance操作。
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition("test-topic", 0));
        topicPartitions.add(new TopicPartition("test-topic", 1));

        // group名称
        String groupId = "test-group";

        // subscribe模式
        ConsumerConfig configs = new ConsumerConfig(boostrapServers, topic, groupId);
        
        // assign模式
        ConsumerConfig configs = new ConsumerConfig(boostrapServers, topicPartitions, groupId);

        // key 反序列化器，必须指定
        configs.setKeySerializer(SerializerImpl.getStringSerializer());

        // value 反序列化器，必须指定
        configs.setValueSerializer(SerializerImpl.getStringSerializer());
        
        //
        configs.setAutoCommit(true);

        // 创建一个封装用户处理逻辑的MessageListener，注意该对象必须是无状态的
        MessageListener<String, String> messageListener = new AutoCommitMessageListener<String, String>() {
            @Override
            public void onMessage(Record<String, String> record) {
                // 强烈建议捕获异常
                try {
                    String topic = record.getTopic();
                    int partition = record.getPartition();
                    long offset = record.getOffset();
                    long timestamp = record.getTimestamp();
                    String key = record.getKey();
                    String value = record.getValue();
                    // do something

                } catch (Exception e) {
                    // log exception.
                }
            }
        };

        // 构建KafkaConsumer，必须在setMessageListener后start
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }
}
```
**参数说明**
```
//对于常用的属性，进行了封装:
configs.setAutoCommit(true);
configs.setAutoCommitInterval(5000);

//对于不常用的属性，通过put方式设置
configs.put("max.poll.records", "500");
```
**注意事项**
1. KafkaConsumer为线程安全。
2. configs.setParallelism()和setPartitionOrderly()同时设置时，和setPartitionOrderly优先级更高，即内部消费线程数为当前consumer获取的分区数。当kafka发生relance后，线程数会随着新分配的分区进行增加或减少。


```java
//手动提交offset
public class AcknowledgeConsumerExample {

    public static void main(String[] args) {

        // Kafka集群地址
        String boostrapServers = "localhost:9092";

        // 订阅topic，只支持单个topic。当指定topic后，内部为subscribe模式，支持rebalance操作。
        String topic = "test-topic";
        
        // 订阅topic和分区。当指定TopicPartition后，内部为assign模式，不支持rebalance操作。
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition("test-topic", 0));
        topicPartitions.add(new TopicPartition("test-topic", 1));

        // group名称
        String groupId = "test-group";

        // subscribe模式
        ConsumerConfig configs = new ConsumerConfig(boostrapServers, topic, groupId);
        
        // assign模式
        ConsumerConfig configs = new ConsumerConfig(boostrapServers, topicPartitions, groupId);

        // key 反序列化器，必须指定
        configs.setKeySerializer(SerializerImpl.getStringSerializer());

        // value 反序列化器，必须指定
        configs.setValueSerializer(SerializerImpl.getStringSerializer());

        // 创建一个封装用户处理逻辑的MessageListener
        MessageListener<String, String> messageListener = new AcknowledgeMessageListener<String, String>() {
            @Override
            public void onMessage(Record<String, String> record, Acknowledgment acknowledgment) {
                // 强烈建议捕获异常
                try {
                    String topic = record.getTopic();
                    int partition = record.getPartition();
                    long offset = record.getOffset();
                    long timestamp = record.getTimestamp();
                    String key = record.getKey();
                    String value = record.getValue();

                    // do something.
                } catch (Exception e) {
                    // log exception.
                } finally {
                    // 手动提交模式下必须要调用该方法
                    acknowledgment.acknowledge();
                }
            }
        };

        // 构建KafkaConsumer，必须在setMessageListener后start
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }
}
```

```java
/**
 * 手动提交batch模式下，收到消息单位为一组，而不是一个。
 */
public class BatchAcknowledgeConsumerExample {

    public static void main(String[] args) {

        // Kafka集群地址
        String boostrapServers = "localhost:9092";

        // 订阅topic，只支持单个topic。 当指定topic后，内部为subscribe模式，支持rebalance操作。
        String topic = "test-topic";
        
        // 订阅topic和分区。当指定TopicPartition后，内部为assign模式，不支持rebalance操作。
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition("test-topic", 0));
        topicPartitions.add(new TopicPartition("test-topic", 1));

        // group名称
        String groupId = "test-group";

        // subscribe模式
        ConsumerConfig configs = new ConsumerConfig(boostrapServers, topic, groupId);
        
        // assign模式
        ConsumerConfig configs = new ConsumerConfig(boostrapServers, topicPartitions, groupId);

        // key 反序列化器，必须指定
        configs.setKeySerializer(SerializerImpl.serializerImpl());

        // value 反序列化器，必须指定
        configs.setValueSerializer(SerializerImpl.serializerImpl());

        // 创建一个封装用户处理逻辑的MessageListener
        MessageListener<String, String> messageListener = new BatchAcknowledgeMessageListener<String, String>() {
            @Override
            public void onMessage(List<Record<String, String>> records, Acknowledgment acknowledgment) {
                // 强烈建议捕获异常
                try {
                    //多线程or单线程处理records
                    //注意，只有执行完acknowledgment.acknowledge()才会再次的收到下批消息。
                } catch (Exception e) {
                    // log exception.
                } finally {
                    // 手动提交模式下必须要调用该方法
                    acknowledgment.acknowledge();
                }
            }
        };

        // 构建KafkaConsumer，必须在setMessageListener后start
        KafkaConsumer<String, String> consumer = OwlKafkaClient.createConsumer(configs);
        consumer.setMessageListener(messageListener);
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close(); //程序关闭时调用。
        }));
    }
}
```

|参数|说明|
|:----:|:----:|
|acknowledgeCommitBatchSize|手动提交批量的大小|
|acknowledgeCommitInterval|手动提交的间隔时间|
|partitionOrderly|设置是否为分区有序消费|
|parallelism|并发消费的线程数|
|handlerQueueSize|内存缓冲队列的大小|
|batchConsumeSize|batch模式消费下，每次消息的批量大小|
|batchConsumeTime|batch模式消费下，每次消费消息的间隔时间|

**注意事项**
1. KafkaConsumer为线程安全。
2. 设置setPartitionOrderly(true)后，内部消费线程数为当前consumer获取的分区数。 当kafka发生relance后，线程数会随着新分配的分区进行增加或减少。

#### 五. 序列化
1. 默认提供了六种序列化方式，JacksonSerializer，HessianSerializer，ByteArraySerializer， StringSerializer，FastJsonSerializer，ProtoStuffSerializer。
2. 使用内部的序列化方式，可以SerializerImpl.serializerImpl()获取，SPI式，默认为bytearray。或者使用SerializerImpl.getHessianSerializer()等获取其他序列化方式; 
3. ByteArraySerializer为空实现，当consumer/producer的key或value为byte数组时，请使用此序列化方式。
4. 如想实现其他序列化方式，实现Serializer接口即可。

#### 六. 注意项
1. MessageListener 接口必须保证是无状态的，内部会有多个线程同时调用onMessage方法。
2. MessageListener#onMessage方法使用时必须要做异常捕获与处理，在抛出throwable后不会停止整个处理流程。
3. AutoCommitMessageListener不适合对数据可靠性要求非常高的处理场景，在日志数据等容忍少量丢失的情况下可以使用该类型，如果不要求分区有序性，建议使用AutoCommitMessageListener同时配置多个处理线程，能保证每个处理线程负载相同。
4. AcknowledgeMessageListener能保证至少一次的语义，不会丢失数据，在consumer重启，reblance时，会出现重复消费的问题，使用时可以在消费端做幂等性处理。
5. 如果配置了partitionOrderly选项，能保证对于单个partition的顺序处理，但如果各个partition的负载不同，会导致热点partition的处理线程处理能力饱和，进而影响整个consumer端处理吞吐的下降，强烈建议在producer端做好分发均衡策略。
6. 当消费的topic不存在时，客户端会报topic [ XXX ] not exist异常并终止consumer。这是为了防止相同的groupId订阅了不同topic后，只要有一个topic未创建，就会导致发生rebalancing，只要要持续5分钟。终止consumer后，可以不影响其他topic的消费。

#### 七. 性能压测
1. 性能压测后，数据接近原生api，占用极小的应用内存。
