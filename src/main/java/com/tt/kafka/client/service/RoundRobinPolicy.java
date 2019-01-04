package com.tt.kafka.client.service;

import com.tt.kafka.client.transport.Address;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Tboy
 */
public class RoundRobinPolicy implements LoadBalancePolicy<Address> {

    private final AtomicInteger index = new AtomicInteger(0);

    private final RegistryService registryService;

    public RoundRobinPolicy(RegistryService registryService){
        this.registryService = registryService;
    }

    @Override
    public Address get() {
        List<Address> providers = registryService.getProviders();
        if(providers.size() <= 0){
            return null;
        }
        return providers.get(index.incrementAndGet() % providers.size());
    }
}
