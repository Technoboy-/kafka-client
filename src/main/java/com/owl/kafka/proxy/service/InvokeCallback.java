package com.owl.kafka.proxy.service;

/**
 * @Author: Tboy
 */
public interface InvokeCallback {

    void onComplete(final InvokerPromise invokerPromise);
}
