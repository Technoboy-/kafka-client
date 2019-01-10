package com.tt.kafka.client.transport;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.client.transport.codec.PacketDecoder;
import com.tt.kafka.client.transport.codec.PacketEncoder;
import com.tt.kafka.client.transport.handler.ClientHandler;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.NamedThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
abstract class Transporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(Transporter.class);

    protected Bootstrap bootstrap;

    private NioEventLoopGroup workGroup;

    public Transporter(int workNum){
        bootstrap = new Bootstrap();
        workGroup = new NioEventLoopGroup(workNum);
    }

    protected void initHandler(ChannelHandler handler) {
        bootstrap.
                option(ChannelOption.SO_KEEPALIVE, true).
                option(ChannelOption.TCP_NODELAY, true).
                group(workGroup).
                channel(NioSocketChannel.class).
                handler(new ChannelInitializer<NioSocketChannel>() {

                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        //out
                        pipeline.addLast("encoder", new PacketEncoder());

                        //in
                        pipeline.addLast("decoder", new PacketDecoder());
                        pipeline.addLast("clientHandler", handler);
                    }
                });
    }

    public void close(){
        if(workGroup != null){
            workGroup.shutdownGracefully();
        }
        LOGGER.debug("close Transporter");
    }

}
