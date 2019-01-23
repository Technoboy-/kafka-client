package com.owl.kafka.client.proxy.service;

/**
 * @Author: Tboy
 */
public interface InvokeCallback {

    void onComplete(final InvokerPromise invokerPromise);
}
