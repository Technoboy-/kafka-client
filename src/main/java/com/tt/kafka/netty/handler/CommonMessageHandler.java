package com.tt.kafka.netty.handler;

import com.tt.kafka.netty.protocol.Packet;
import com.tt.kafka.netty.transport.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public abstract class CommonMessageHandler implements MessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonMessageHandler.class);

    @Override
    public void beforeHandle(Connection connection, Packet packet) throws Exception {
        //NOP
    }

    @Override
    public void afterHandle(Connection connection, Packet packet) throws Exception {
        //NOP
    }
}
