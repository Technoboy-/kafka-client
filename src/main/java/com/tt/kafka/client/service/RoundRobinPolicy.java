package com.tt.kafka.client.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Tboy
 */
public class RoundRobinPolicy implements LoadBalancePolicy<Address> {

    private final AtomicInteger index = new AtomicInteger(0);

    private final DiscoveryService discoveryService;

    public RoundRobinPolicy(DiscoveryService discoveryService){
        this.discoveryService = discoveryService;
    }

    @Override
    public Address get() {
        List<Address> providers = discoveryService.getProviders();
        if(providers.size() <= 0){
            return null;
        }
        return providers.get(index.incrementAndGet() % providers.size());
    }
}
