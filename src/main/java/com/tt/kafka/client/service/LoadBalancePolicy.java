package com.tt.kafka.client.service;

/**
 * @Author: Tboy
 */
public interface LoadBalancePolicy<T> {

    T get();
}
