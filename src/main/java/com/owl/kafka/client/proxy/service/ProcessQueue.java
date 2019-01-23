package com.owl.kafka.client.proxy.service;

import com.owl.kafka.client.proxy.transport.message.Message;

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author: Tboy
 */
public class ProcessQueue {

    public static ProcessQueue I = new ProcessQueue();

    private final TreeMap<Long, Message> treeMap = new TreeMap<>();

    private final AtomicLong msgCount = new AtomicLong(0);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public void put(long msgId, Message message){
        this.lock.writeLock().lock();
        try {
            msgCount.incrementAndGet();
            treeMap.put(msgId, message);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void remove(Long msgId){
        this.lock.writeLock().lock();
        try {
            Message remove = treeMap.remove(msgId);
            if(remove != null){
                msgCount.decrementAndGet();
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public long getMessageCount() {
        return msgCount.get();
    }


}
