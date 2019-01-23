package com.owl.kafka.client.proxy.transport.handler;

import com.owl.kafka.client.proxy.transport.NettyConnection;
import com.owl.kafka.client.proxy.util.Packets;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
@ChannelHandler.Sharable
public abstract class ConnectionWatchDog extends ChannelInboundHandlerAdapter implements TimerTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionWatchDog.class);

    private final Bootstrap bootstrap;
    private final Timer timer;
    private final SocketAddress socketAddress;

    private volatile boolean isReconnect = true;

    public ConnectionWatchDog(Bootstrap bootstrap, Timer timer, SocketAddress socketAddress){
        this.bootstrap = bootstrap;
        this.timer = timer;
        this.socketAddress = socketAddress;
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.info("connect to server : {} success", socketAddress);
        //do register
        NettyConnection.attachChannel(ctx.channel());
        ctx.writeAndFlush(Packets.registerContent());
        ctx.fireChannelActive();
    }

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        boolean doReconnect = isReconnect();
        if(doReconnect){
            timer.newTimeout(this, 100, TimeUnit.MILLISECONDS);
        }
        LOGGER.warn("Disconnects with {}, address: {}, reconnect: {}.", new Object[]{ctx.channel(), socketAddress, doReconnect});

        ctx.fireChannelInactive();
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if(!isReconnect()){
            return;
        }
        ChannelFuture future;
        synchronized (bootstrap){
            future = bootstrap.connect(socketAddress);
        }
        future.awaitUninterruptibly(3000, TimeUnit.MILLISECONDS);
        if (future.cause() != null) {
            LOGGER.error("connect to server " + socketAddress + " fail", future.cause());
        }
    }

    public boolean isReconnect() {
        return isReconnect;
    }

    public void setReconnect(boolean reconnect) {
        isReconnect = reconnect;
    }

    public abstract ChannelHandler[] handlers();
}
