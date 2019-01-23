package com.owl.kafka.client.proxy.service;

import com.owl.kafka.client.proxy.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public interface PullCallback {

    void onComplete(Packet response);

    void onException(Throwable ex);
}
