package com.owl.kafka.client.proxy.transport;

import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelId;

import java.net.SocketAddress;

/**
 * @Author: Tboy
 */
public interface Connection {

    ChannelId getId();

    boolean isActive();

    boolean isWritable();

    void close();

    Channel getChannel();

    void send(Packet packet) throws ChannelInactiveException;

    void send(Packet packet, ChannelFutureListener listener) throws ChannelInactiveException;

    SocketAddress getRemoteAddress();

    long getConnectTime();

}
