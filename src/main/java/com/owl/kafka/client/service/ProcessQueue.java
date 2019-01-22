package com.owl.kafka.client.service;

import com.owl.kafka.client.transport.message.Message;
import com.owl.kafka.client.transport.protocol.Packet;

import java.util.Map;
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

    public Message take(){
        this.lock.writeLock().lock();
        try {
            if(!treeMap.isEmpty()){
                Map.Entry<Long, Message> entry = treeMap.pollFirstEntry();
                if(entry != null){
                    msgCount.decrementAndGet();
                    return entry.getValue();
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }
        return null;
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

    public long getMaxSpan() {
        this.lock.readLock().lock();
        try {
            if(!treeMap.isEmpty()){
                return treeMap.lastKey() - treeMap.firstKey();
            }
            return 0;

        } finally {
            this.lock.readLock().unlock();
        }
    }

    public long getMessageCount() {
        return msgCount.get();
    }


}
