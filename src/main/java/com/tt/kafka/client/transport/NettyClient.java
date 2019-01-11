package com.tt.kafka.client.transport;

import com.tt.kafka.client.transport.codec.PacketDecoder;
import com.tt.kafka.client.transport.codec.PacketEncoder;
import com.tt.kafka.client.transport.handler.*;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.util.Unregisters;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.NamedThreadFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class NettyClient extends NettyConnector{

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private final IdleStateTrigger idleStateTrigger = new IdleStateTrigger();

    private final PacketEncoder encoder = new PacketEncoder();

    private final HashedWheelTimer timer = new HashedWheelTimer(new NamedThreadFactory("connector.timer"));

    private final MessageDispatcher dispatcher = new MessageDispatcher();

    private final ClientHandler handler;

    public NettyClient(MessageListenerService messageListenerService){
        this.dispatcher.register(Command.PUSH, new PushMessageHandler(messageListenerService));
        this.handler = new ClientHandler(this.dispatcher);
    }

    public void connect(InetSocketAddress address) {
        ChannelFuture future = null;
        Channel channel = null;
        try {
            final ConnectionWatchDog connectionWatchDog = new ConnectionWatchDog(bootstrap, timer, address){

                @Override
                public ChannelHandler[] handlers() {
                    return new ChannelHandler[]{this,
                            new IdleStateHandler(60, 60, 9),
                            idleStateTrigger, new PacketDecoder(), handler, encoder};
                }
            };
            synchronized (bootstrap){
                bootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast((connectionWatchDog.handlers()));
                    }
                });
                future = bootstrap.connect(address);
            }
            boolean connected = future.awaitUninterruptibly(3000, TimeUnit.MILLISECONDS);
            if (connected && future.isSuccess()) {
                channel = future.channel();
                channels.put(address, channel);
                LOGGER.info("connect to server : {} success", address);
            } else if (future.cause() != null) {
                LOGGER.error("connect to server " + address + " fail", future.cause());
            } else {
                LOGGER.error("connect to server " + address + " fail", future.cause());
            }
        } finally {
            if (!isConnected(channel) && future != null) {
                future.cancel(true);
            }
        }
    }

    protected boolean isConnected(Channel channel) {
        if (channel != null) {
            return channel.isActive();
        }
        return false;
    }

    public void disconnect(InetSocketAddress address){
        try {
            Channel channel = channels.remove(address);
            if(channel != null){
                channel.writeAndFlush(Unregisters.unregister());
                channel.close();
            }
        } catch (Throwable ex){
            LOGGER.error("disconnect error", ex);
        }
    }

    public void close(){
        for(InetSocketAddress address : channels.keySet()){
            disconnect(address);
        }
        super.close();
    }
}
