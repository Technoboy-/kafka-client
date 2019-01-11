package com.tt.kafka.client.transport;

import com.tt.kafka.client.SystemPropertiesUtils;
import com.tt.kafka.util.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
abstract class NettyConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyConnector.class);

    protected final ConcurrentHashMap<InetSocketAddress, Channel> channels = new ConcurrentHashMap<>();

    protected Bootstrap bootstrap = new Bootstrap();

    private final int workerNum = SystemPropertiesUtils.getInt(Constants.PUSH_CLIENT_WORKER_NUM, Constants.CPU_SIZE);

    private final NioEventLoopGroup workGroup = new NioEventLoopGroup(workerNum);

    public NettyConnector(){
        bootstrap.
                option(ChannelOption.SO_KEEPALIVE, true).
                option(ChannelOption.TCP_NODELAY, true).
                group(workGroup).
                channel(NioSocketChannel.class);
    }

    public void close(){
        if(workGroup != null){
            workGroup.shutdownGracefully();
        }
        LOGGER.debug("close NettyConnector");
    }

}
