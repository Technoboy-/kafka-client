package com.owl.kafka.proxy.service;

/**
 * @Author: Tboy
 */
public interface RetryPolicy {

    boolean allowRetry() throws InterruptedException;

    void reset();

}
