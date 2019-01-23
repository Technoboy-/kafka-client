package com.owl.kafka.proxy.service;

import com.owl.kafka.proxy.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public interface PullCallback {

    void onComplete(Packet response);

    void onException(Throwable ex);
}
