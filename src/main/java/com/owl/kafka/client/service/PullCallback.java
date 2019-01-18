package com.owl.kafka.client.service;

import com.owl.kafka.client.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public interface PullCallback {

    void onComplete(Packet response);

    void onException(Throwable ex);
}
