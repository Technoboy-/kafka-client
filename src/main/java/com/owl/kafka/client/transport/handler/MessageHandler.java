package com.owl.kafka.client.transport.handler;

import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public interface MessageHandler {

    void handle(Connection connection, Packet packet) throws Exception;

    void beforeHandle(Connection connection, Packet packet) throws Exception;

    void afterHandle(Connection connection, Packet packet) throws Exception;

}
