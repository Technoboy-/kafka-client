package com.tt.kafka.client.transport.handler;

import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.transport.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class HeartbeatHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatHandler.class);

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        //TODO
    }

}
