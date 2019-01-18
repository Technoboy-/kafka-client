package com.owl.kafka.client.service;

/**
 * @Author: Tboy
 */
public interface InvokeCallback {

    void onComplete(final InvokerPromise invokerPromise);
}
