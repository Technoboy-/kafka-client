package com.tt.kafka.client.service;

import com.tt.kafka.client.transport.Address;

/**
 * @Author: Tboy
 */
public interface RegistryListener<T> {

    void onSubscribe(String path);

    void onRegister(RegisterMetadata<T> metadata);

    void onDestroy(RegisterMetadata<T> metadata);

    void onChange(Address address, Event event);

    enum Event{

        ADD,

        DELETE;
    }
}
