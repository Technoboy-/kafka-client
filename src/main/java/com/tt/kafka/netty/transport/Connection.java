package com.tt.kafka.netty.transport;

import com.tt.kafka.netty.protocol.Packet;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelId;

/**
 * @Author: Tboy
 */
public interface Connection {

    ChannelId getId();

    boolean isActive();

    void close();

    Channel getChannel();

    void send(Packet packet);

    void send(Packet packet, ChannelFutureListener listener);

    long getConnectTime();

}
