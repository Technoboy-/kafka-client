package com.tt.kafka.client.transport;

import com.tt.kafka.client.transport.handler.ConnectionWatchDog;
import io.netty.channel.ChannelFuture;

/**
 * @Author: Tboy
 */
public class Reconnector {

    private final ConnectionWatchDog connectionWatchDog;

    private final ChannelFuture future;

    public Reconnector(ConnectionWatchDog connectionWatchDog, ChannelFuture future){
        this.connectionWatchDog = connectionWatchDog;
        this.future = future;
    }

    public void setReconnect(boolean reconnect){
        connectionWatchDog.setReconnect(reconnect);
    }


}
