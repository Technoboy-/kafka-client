package com.owl.kafka.client.service;

import com.owl.kafka.client.transport.protocol.Packet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class InvokerPromise {

    private static final ConcurrentHashMap<Long, InvokerPromise> promises = new ConcurrentHashMap<>();

    private final long msgId;

    private final long startMs;

    private final long timeoutMs;

    private final CountDownLatch latch;

    private Packet response;

    private InvokeCallback invokeCallback;

    public InvokerPromise(long msgId, long timeoutMs){
        this.msgId = msgId;
        this.startMs = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
        this.latch = new CountDownLatch(1);
        promises.put(this.msgId, this);
    }

    public InvokerPromise(long msgId, long timeoutMs, InvokeCallback invokeCallback){
        this.msgId = msgId;
        this.startMs = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
        this.invokeCallback = invokeCallback;
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

    public static InvokerPromise get(long msgId){
        return promises.get(msgId);
    }

    public void doReceive(Packet packet){
        this.response = packet;
        this.latch.countDown();
    }

    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            invokeCallback.onComplete(this);

        }
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public long getStartMs() {
        return startMs;
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() - getStartMs() > getTimeoutMs();
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public static void scan(){
        final List<InvokerPromise> removeList = new ArrayList<>();
        Iterator<Map.Entry<Long, InvokerPromise>> iterator = promises.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<Long, InvokerPromise> next = iterator.next();
            InvokerPromise invokerPromise = next.getValue();
            if(invokerPromise.getStartMs() + invokerPromise.getTimeoutMs() <= System.currentTimeMillis()){
                iterator.remove();
                removeList.add(invokerPromise);
            }
        }
        for(InvokerPromise invokerPromise : removeList){
            invokerPromise.executeInvokeCallback();
        }
    }
}
