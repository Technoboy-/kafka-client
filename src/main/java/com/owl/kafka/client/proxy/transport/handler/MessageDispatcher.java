package com.owl.kafka.client.proxy.transport.handler;

import com.owl.kafka.client.proxy.transport.protocol.Command;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Tboy
 */
public class MessageDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageDispatcher.class);

    private Map<Byte, MessageHandler> handlers = new HashMap<Byte, MessageHandler>();

    public void dispatch(Connection connection, Packet packet) {
        MessageHandler messageHandler = handlers.get(packet.getCmd());
        try {
            if(messageHandler != null){
                messageHandler.beforeHandle(connection, packet);
                messageHandler.handle(connection, packet);
                messageHandler.afterHandle(connection, packet);
            } else{
                LOGGER.warn("invalid msg cmd {}, close channel {} ", packet.getCmd(), NetUtils.getRemoteAddress(connection.getChannel()));
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("dispatch msg {} , error {}, close channel {}", new Object[]{packet, e, NetUtils.getRemoteAddress(connection.getChannel())});
            connection.close();
        }
    }

    public void register(Command command, MessageHandler messageHandler){
        handlers.put(command.cmd, messageHandler);
    }
}
