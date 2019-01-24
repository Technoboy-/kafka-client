package com.owl.kafka.client.proxy.service;

import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.util.Packets;
import com.owl.kafka.client.util.NamedThreadFactory;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @Author: Tboy
 */
public class OffsetStore {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetStore.class);

    public static OffsetStore I = new OffsetStore();

    protected final ScheduledExecutorService commitScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("commit-scheduler"));

    protected volatile ConcurrentMap<TopicPartition, TopicPartitionOffset> latestOffsetMap = new ConcurrentHashMap<>();

    private final OffsetQueue offsetQueue = new OffsetQueue();

    private Connection connection;

    public void storeOffset(List<Message> messages){
        offsetQueue.put(messages);
    }

    public void updateOffset(Connection connection, long msgId){
        this.connection = connection;
        TopicPartitionOffset offset = offsetQueue.remove(msgId);
        if(offset != null){
            TopicPartition topicPartition = new TopicPartition(offset.getTopic(), offset.getPartition());
            TopicPartitionOffset exist = latestOffsetMap.get(topicPartition);
            if (exist == null || offset.getOffset() > exist.getOffset()) {
                latestOffsetMap.put(topicPartition, offset);
            }
        }
    }

    class ScheduledCommitOffsetTask implements Runnable {

        @Override
        public void run() {
            try {
                final Map<TopicPartition, TopicPartitionOffset> pre = latestOffsetMap;
                latestOffsetMap = new ConcurrentHashMap<>();
                if (pre.isEmpty()) {
                    return;
                }
                for(TopicPartitionOffset offset : pre.values()){
                    connection.send(Packets.ackPullReq(offset));
                }
            } catch (Throwable ex) {
                LOG.error("Commit consumer offset error.", ex);
            }
        }
    }

    public long getCount() {
        return offsetQueue.getMessageCount();
    }


    public void close(){
        this.commitScheduler.shutdown();
        if (!latestOffsetMap.isEmpty()) {
            new ScheduledCommitOffsetTask().run();
        }
    }
}
