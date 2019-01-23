package com.owl.kafka.client.proxy.service;

import com.owl.kafka.client.proxy.transport.Address;

/**
 * @Author: Tboy
 */
public interface RegistryListener {

    void onChange(Address address, Event event);

    enum Event{

        ADD,

        DELETE;
    }
}
