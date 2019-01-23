package com.owl.kafka.client.proxy.transport.handler;

import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public interface MessageHandler {

    void handle(Connection connection, Packet packet) throws Exception;

    void beforeHandle(Connection connection, Packet packet) throws Exception;

    void afterHandle(Connection connection, Packet packet) throws Exception;

}
