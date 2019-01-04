package com.tt.kafka.client.service;

/**
 * @Author: Tboy
 */
public interface RetryPolicy {

    boolean allowRetry() throws InterruptedException;

    void reset();

}
