package com.owl.kafka.client.proxy.transport;

import com.owl.kafka.client.proxy.transport.alloc.ByteBufferPool;
import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.protocol.Command;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.util.NetUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelId;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Date;
import java.util.Objects;

/**
 * @Author: Tboy
 */
public class NettyConnection implements Connection {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyConnection.class);

    private static final AttributeKey<NettyConnection> channelKey = AttributeKey.valueOf("channel.key");

    private static final ByteBufferPool bufferPool = ByteBufferPool.DEFAULT;

    private final Channel channel;

    private final long connectTime;

    public static NettyConnection attachChannel(Channel channel){
        Attribute<NettyConnection> attr = channel.attr(channelKey);
        NettyConnection oldChannel = attr.get();
        if(oldChannel == null){
            NettyConnection nettyChannel = new NettyConnection(channel);
            oldChannel = attr.setIfAbsent(nettyChannel);
            if(oldChannel == null){
                oldChannel = nettyChannel;
            }
        }
        return oldChannel;
    }

    private NettyConnection(Channel channel){
        this.channel = channel;
        this.connectTime = new Date().getTime();
    }

    @Override
    public ChannelId getId() {
        return this.channel.id();
    }

    @Override
    public boolean isActive() {
        return this.channel.isActive();
    }

    @Override
    public boolean isWritable() {
        return this.channel.isWritable();
    }

    @Override
    public void close() {
        this.channel.close();
    }

    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public void send(Packet packet) throws ChannelInactiveException {
        this.send(packet, null);
    }

    @Override
    public void send(Packet packet, ChannelFutureListener listener) throws ChannelInactiveException {
        if(this.channel.isActive()){
            ChannelFuture future = this.channel.writeAndFlush(packet).addListener(new ChannelFutureListener(){

                public void operationComplete(ChannelFuture future) throws Exception {
                    bufferPool.release(packet.getBody());
                    //
                    if(future.isSuccess()){
                        if(!isHeartbeat(packet)){
                            LOGGER.debug("send msg {} , to : {}, successfully", packet, NetUtils.getRemoteAddress(channel));
                        }
                    } else{
                        future.cause().printStackTrace();
                        LOGGER.error("send msg {} failed, error {}", packet, future.cause());
                    }
                }
            });
            if(listener != null){
                future.addListener(listener);
            }
        } else{
            throw new ChannelInactiveException("channel inactive exception, msg : { " + packet + " }");
        }
    }

    private boolean isHeartbeat(Packet packet){
        Command command = Command.toCMD(packet.getCmd());
        return Command.PING == command || Command.PONG == command;
    }

    @Override
    public long getConnectTime() {
        return this.connectTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NettyConnection that = (NettyConnection) o;
        return Objects.equals(getChannel().id().asLongText(), that.getChannel().id().asLongText());
    }

    @Override
    public SocketAddress getRemoteAddress(){
        return this.channel.remoteAddress();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChannel().id().asLongText());
    }
}
