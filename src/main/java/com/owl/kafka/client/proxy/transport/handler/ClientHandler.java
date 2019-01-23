package com.owl.kafka.client.proxy.transport.handler;

import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.NettyConnection;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
@Sharable
public class ClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientHandler.class);

    private final MessageDispatcher dispatcher;

    public ClientHandler(MessageDispatcher dispatcher){
        this.dispatcher = dispatcher;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Connection connnection = NettyConnection.attachChannel(ctx.channel());
        connnection.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        dispatcher.dispatch(NettyConnection.attachChannel(ctx.channel()), (Packet)msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error("exceptionCaught", cause);
        ctx.close();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        Channel ch = ctx.channel();
        ChannelConfig config = ch.config();

        if (!ch.isWritable()) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("{} is not writable, over high water level : {}",
                        new Object[]{ch, config.getWriteBufferHighWaterMark()});
            }

            config.setAutoRead(false);
        } else {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("{} is writable, to low water : {}",
                        new Object[]{ch, config.getWriteBufferLowWaterMark()});
            }

            config.setAutoRead(true);
        }
    }

}
