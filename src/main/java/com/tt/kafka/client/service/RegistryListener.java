package com.tt.kafka.client.service;

/**
 * @Author: Tboy
 */
public interface RegistryListener<T> {

    void onSubscribe(String path);

    void onRegister(RegisterMetadata<T> metadata);

    void onDestroy(RegisterMetadata<T> metadata);
}
