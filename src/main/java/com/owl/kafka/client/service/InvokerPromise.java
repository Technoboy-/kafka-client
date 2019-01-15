package com.owl.kafka.client.service;

import com.owl.kafka.client.transport.protocol.Packet;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class InvokerPromise {

    private static final ConcurrentHashMap<Long, InvokerPromise> promises = new ConcurrentHashMap<>();

    private final long msgId;

    private final long timeoutMs;

    private final CountDownLatch latch;

    private Packet response;

    public InvokerPromise(long msgId, long timeoutMs){
        this.msgId = msgId;
        this.timeoutMs = timeoutMs;
        this.latch = new CountDownLatch(1);
        promises.put(this.msgId, this);
    }


    public Packet getResult(){
        try {
            latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex){
            //Ignore
        }
        return response;
    }

    public static void receive(Packet packet){
        InvokerPromise promise = promises.remove(packet.getMsgId());
        if(promise != null){
            promise.doReceive(packet);
        }
    }

    private void doReceive(Packet packet){
        this.response = packet;
        this.latch.countDown();
    }
}
