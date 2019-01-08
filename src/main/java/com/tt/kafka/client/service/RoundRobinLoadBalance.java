package com.tt.kafka.client.service;

import com.tt.kafka.client.transport.Address;
import com.tt.kafka.client.transport.Connection;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Tboy
 */
public class RoundRobinLoadBalance implements LoadBalance<Address> {

    private final AtomicInteger index = new AtomicInteger(0);

    public Address select(List<Address> invokers) {
        if(invokers.size() <= 0){
            return null;
        }
        if(index.get() >= invokers.size()){
            index.set(0);
        }
        Address address = invokers.get(index.get());
        index.incrementAndGet();
        return address;
    }
}
