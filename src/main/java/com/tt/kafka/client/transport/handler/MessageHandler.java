package com.tt.kafka.client.transport.handler;

import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.transport.Connection;

/**
 * @Author: Tboy
 */
public interface MessageHandler {

    void handle(Connection connection, Packet packet) throws Exception;

    void beforeHandle(Connection connection, Packet packet) throws Exception;

    void afterHandle(Connection connection, Packet packet) throws Exception;

}
