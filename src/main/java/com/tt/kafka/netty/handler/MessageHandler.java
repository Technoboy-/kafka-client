package com.tt.kafka.netty.handler;

import com.tt.kafka.netty.protocol.Packet;
import com.tt.kafka.netty.transport.Connection;

/**
 * @Author: Tboy
 */
public interface MessageHandler {

    public void handle(Connection connection, Packet packet) throws Exception;

    public void beforeHandle(Connection connection, Packet packet) throws Exception;

    public void afterHandle(Connection connection, Packet packet) throws Exception;

}
