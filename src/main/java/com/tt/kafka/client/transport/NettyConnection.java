package com.tt.kafka.client.transport;

import com.tt.kafka.client.service.IdService;
import com.tt.kafka.client.service.LoadBalance;
import com.tt.kafka.client.service.RoundRobinLoadBalance;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.util.NetUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelId;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class NettyConnection implements Connection {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyConnection.class);

    private static final AttributeKey<NettyConnection> channelKey = AttributeKey.valueOf("channel.key");

    private final Channel channel;

    private final long connectTime;

    private ScheduledExecutorService heartbeatService;

    public static NettyConnection attachChannel(Channel channel, boolean clientSide){
        Attribute<NettyConnection> attr = channel.attr(channelKey);
        NettyConnection oldChannel = attr.get();
        if(oldChannel == null){
            NettyConnection nettyChannel = new NettyConnection(channel, clientSide);
            oldChannel = attr.setIfAbsent(nettyChannel);
            if(oldChannel == null){
                oldChannel = nettyChannel;
            }
        }
        return oldChannel;
    }

    private NettyConnection(Channel channel, boolean clientSide){
        this.channel = channel;
        this.connectTime = new Date().getTime();
        if(clientSide){
            this.heartbeatService = Executors.newSingleThreadScheduledExecutor();
            this.heartbeatService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        send(heartbeatPacket());
                    } catch (Exception ex){
                        LOGGER.error("HeartbeatTask error {}, close channel ", ex);
                        close();
                    }

                }
            }, 10, 10, TimeUnit.SECONDS);
            this.send(registerPacket());
        }
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
    public void close() {
        if(this.heartbeatService != null){
            this.heartbeatService.shutdown();
        }
        this.channel.close();
    }

    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public void send(Packet packet) {
        this.send(packet, null);
    }

    @Override
    public void send(Packet packet, ChannelFutureListener listener) {
        if(this.channel.isActive()){
            ChannelFuture future = this.channel.writeAndFlush(packet).addListener(new ChannelFutureListener(){

                public void operationComplete(ChannelFuture future) throws Exception {
                    if(future.isSuccess()){
                        LOGGER.info("send msg {} , to clientId : {}, successfully",packet, NetUtils.getRemoteAddress(channel));
                    } else{
                        LOGGER.error("send msg {} failed, error {}", packet, future.cause());
                    }
                }
            });
            if(listener != null){
                future.addListener(listener);
            }
        } else{
            throw new RuntimeException("channel inactive exception, msg : { " + packet + " }");
        }
    }

    @Override
    public long getConnectTime() {
        return this.connectTime;
    }


    private Packet registerPacket(){
        Packet packet = new Packet();
        packet.setMsgId(IdService.I.getId());
        packet.setCmd(Command.REGISTER.getCmd());
        packet.setHeader(new byte[0]);
        packet.setKey(new byte[0]);
        packet.setValue(new byte[0]);
        return packet;
    }

    private Packet heartbeatPacket(){
        Packet packet = new Packet();
        packet.setMsgId(IdService.I.getId());
        packet.setCmd(Command.HEARTBEAT.getCmd());
        packet.setHeader(new byte[0]);
        packet.setKey(new byte[0]);
        packet.setValue(new byte[0]);
        return packet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NettyConnection that = (NettyConnection) o;
        return Objects.equals(getChannel(), that.getChannel());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChannel());
    }
}
