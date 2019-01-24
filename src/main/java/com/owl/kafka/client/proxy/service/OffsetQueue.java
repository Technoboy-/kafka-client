package com.owl.kafka.client.proxy.service;

import com.owl.kafka.client.proxy.transport.message.Message;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: Tboy
 */
public class OffsetQueue {

    private final ConcurrentHashMap<Long/* msgId */, TopicPartitionOffset> msgIdMap = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer/* parition */, TreeSet<TopicPartitionOffset>> partitionMap = new ConcurrentHashMap<>();

    private final AtomicLong msgCount = new AtomicLong(0);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public void put(List<Message> messages){
        this.lock.writeLock().lock();
        try {
            for(Message message : messages){
                msgCount.incrementAndGet();
                TreeSet<TopicPartitionOffset> offsetTreeSet = partitionMap.get(message.getHeader().getPartition());
                if(offsetTreeSet == null){
                    offsetTreeSet = new TreeSet<>();
                    TreeSet<TopicPartitionOffset> old = partitionMap.putIfAbsent(message.getHeader().getPartition(), offsetTreeSet);
                    if(old != null){
                        offsetTreeSet = old;
                    }
                }
                TopicPartitionOffset topicPartitionOffset = new TopicPartitionOffset(message.getHeader().getTopic(),
                        message.getHeader().getPartition(), message.getHeader().getOffset(), message.getHeader().getMsgId());
                offsetTreeSet.add(topicPartitionOffset);
                msgIdMap.put(message.getHeader().getMsgId(), topicPartitionOffset);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public TopicPartitionOffset remove(Long msgId){
        TopicPartitionOffset result = null;
        this.lock.writeLock().lock();
        try {
            TopicPartitionOffset topicPartitionOffset = msgIdMap.remove(msgId);
            if(topicPartitionOffset != null){
                TreeSet<TopicPartitionOffset> offsetTreeMap = partitionMap.get(topicPartitionOffset.getPartition());
                if(offsetTreeMap != null){
                    offsetTreeMap.remove(topicPartitionOffset);
                }
                if(!offsetTreeMap.isEmpty()){
                    result = offsetTreeMap.first();
                }
                msgCount.decrementAndGet();
            }
        } finally {
            this.lock.writeLock().unlock();
        }
        return result;
    }

    public long getMessageCount() {
        return msgCount.get();
    }


}
