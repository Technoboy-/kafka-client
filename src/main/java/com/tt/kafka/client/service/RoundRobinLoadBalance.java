package com.tt.kafka.client.service;

import com.tt.kafka.client.transport.Address;

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
        return invokers.get(index.incrementAndGet() % invokers.size());
    }
}
