package com.tt.kafka.client.netty.handler;

import com.tt.kafka.client.netty.protocol.Packet;
import com.tt.kafka.client.netty.transport.Connection;

/**
 * @Author: Tboy
 */
public interface MessageHandler {

    public void handle(Connection connection, Packet packet) throws Exception;

    public void beforeHandle(Connection connection, Packet packet) throws Exception;

    public void afterHandle(Connection connection, Packet packet) throws Exception;

}
