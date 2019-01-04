package com.tt.kafka.client.transport.handler;

import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.transport.Connection;

/**
 * @Author: Tboy
 */
public interface MessageHandler {

    public void handle(Connection connection, Packet packet) throws Exception;

    public void beforeHandle(Connection connection, Packet packet) throws Exception;

    public void afterHandle(Connection connection, Packet packet) throws Exception;

}
