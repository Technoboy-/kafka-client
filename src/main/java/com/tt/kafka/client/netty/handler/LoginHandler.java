package com.tt.kafka.client.netty.handler;

import com.tt.kafka.client.netty.protocol.Packet;
import com.tt.kafka.client.netty.service.HeartbeatService;
import com.tt.kafka.client.netty.transport.Connection;
import io.netty.channel.ChannelId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
public class LoginHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoginHandler.class);

    private final Map<ChannelId, HeartbeatService> heartbeatServices = new ConcurrentHashMap<>();

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        HeartbeatService heartbeatService = heartbeatServices.get(connection.getId());
        if(heartbeatService == null){
            heartbeatService = new HeartbeatService(connection);
            HeartbeatService old = heartbeatServices.putIfAbsent(connection.getId(), heartbeatService);
            if(old != null){
                heartbeatService = old;
            }
        }
        heartbeatService.start();
    }

}
