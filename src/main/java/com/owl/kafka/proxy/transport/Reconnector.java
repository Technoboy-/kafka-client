package com.owl.kafka.proxy.transport;

import com.owl.kafka.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.proxy.transport.handler.ConnectionWatchDog;
import com.owl.kafka.proxy.util.Packets;
import io.netty.channel.ChannelFuture;

/**
 * @Author: Tboy
 */
public class Reconnector {

    private final ConnectionWatchDog connectionWatchDog;

    private final Connection connection;

    public Reconnector(ConnectionWatchDog connectionWatchDog, ChannelFuture future){
        this.connectionWatchDog = connectionWatchDog;
        this.connection = NettyConnection.attachChannel(future.channel());
    }

    public Connection getConnection() {
        return connection;
    }

    public void disconnect(){
        this.connectionWatchDog.setReconnect(false);
        this.connection.close();
    }

    public void close(){
        this.connectionWatchDog.setReconnect(false);
        try {
            this.connection.send(Packets.unregister());
        } catch (ChannelInactiveException e) {
            //Ignore
        }
        this.connection.close();
    }
}
