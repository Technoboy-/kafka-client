package com.owl.kafka.client.transport.handler;

import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.protocol.Packet;
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
