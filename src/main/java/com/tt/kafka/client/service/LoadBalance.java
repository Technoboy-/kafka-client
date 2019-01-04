package com.tt.kafka.client.service;

import java.util.List;

/**
 * @Author: Tboy
 */
public interface LoadBalance<T> {

    T select(List<T> invokers);
}
