package com.owl.kafka.client.transport;

import com.owl.kafka.client.transport.handler.ConnectionWatchDog;
import com.owl.kafka.client.util.Packets;
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


    public void disconnect(){
        this.connectionWatchDog.setReconnect(false);
        this.future.channel().close();
    }

    public void close(){
        this.connectionWatchDog.setReconnect(false);
        this.future.channel().writeAndFlush(Packets.unregister());
        this.future.channel().close();
    }
}
