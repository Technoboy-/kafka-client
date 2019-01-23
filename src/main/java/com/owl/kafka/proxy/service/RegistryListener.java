package com.owl.kafka.proxy.service;

import com.owl.kafka.proxy.transport.Address;

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
