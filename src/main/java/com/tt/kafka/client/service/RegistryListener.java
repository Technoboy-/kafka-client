package com.tt.kafka.client.service;

import com.tt.kafka.client.transport.Address;

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
