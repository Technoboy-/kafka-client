package com.owl.kafka.client.proxy.transport;

import com.owl.kafka.client.proxy.ClientConfigs;
import com.owl.kafka.client.proxy.transport.codec.PacketDecoder;
import com.owl.kafka.client.proxy.transport.codec.PacketEncoder;
import com.owl.kafka.client.proxy.transport.handler.*;
import com.owl.kafka.client.proxy.transport.protocol.Command;
import com.owl.kafka.client.consumer.ConsumerConfig;
import com.owl.kafka.client.consumer.service.MessageListenerService;
import com.owl.kafka.client.util.Constants;
import com.owl.kafka.client.util.NamedThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @Author: Tboy
 */
public class NettyClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private final IdleStateTrigger idleStateTrigger = new IdleStateTrigger();

    private final PacketEncoder encoder = new PacketEncoder();

    private final HashedWheelTimer timer = new HashedWheelTimer(new NamedThreadFactory("connector.timer"));

    private final ConnectionManager connectionManager = new ConnectionManager();

    private Bootstrap bootstrap = new Bootstrap();

    private final int workerNum = ClientConfigs.I.getWorkerNum();

    private final NioEventLoopGroup workGroup = new NioEventLoopGroup(workerNum);

    private final MessageDispatcher dispatcher = new MessageDispatcher();

    private ClientHandler handler;

    public NettyClient(MessageListenerService messageListenerService){
        bootstrap.
                option(ChannelOption.SO_KEEPALIVE, true).
                option(ChannelOption.TCP_NODELAY, true).
                group(workGroup).
                channel(NioSocketChannel.class);
        initHandler(messageListenerService);
    }

    private void initHandler(MessageListenerService messageListenerService){
        this.dispatcher.register(Command.PONG, new PongMessageHandler());
        this.dispatcher.register(Command.VIEW_RESP, new ViewMessageHandler());
        //
        String proxyModel = System.getProperty(Constants.PROXY_MODEL);
        if(ConsumerConfig.ProxyModel.PULL == ConsumerConfig.ProxyModel.PULL.valueOf(proxyModel)){
            this.dispatcher.register(Command.PULL_RESP, new PullRespMessageHandler(messageListenerService));
        } else{
            this.dispatcher.register(Command.PUSH, new PushMessageHandler(messageListenerService));
        }
        this.handler = new ClientHandler(this.dispatcher);
    }

    public void connect(Address address, boolean isSync) {
        //
        InetSocketAddress socketAddress = new InetSocketAddress(address.getHost(), address.getPort());
        final ConnectionWatchDog connectionWatchDog = new ConnectionWatchDog(bootstrap, timer, socketAddress){

            @Override
            public ChannelHandler[] handlers() {
                return new ChannelHandler[]{this,
                        new IdleStateHandler(0, 30, 0),
                        idleStateTrigger, new PacketDecoder(), handler, encoder};
            }
        };
        try {
            ChannelFuture future;
            synchronized (bootstrap){
                bootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline().addLast((connectionWatchDog.handlers()));
                    }
                });
                future = bootstrap.connect(socketAddress);
            }
            if(isSync){
               future.sync();
            }
            connectionManager.manage(address, connectionWatchDog);
        } catch (Throwable ex){
            throw new RuntimeException("Connects to [" + address + "] fails", ex);
        }
    }

    public ConnectionManager getConnectionManager() {
        return this.connectionManager;
    }

    public void disconnect(Address address){
        try {
            connectionManager.disconnect(address);
        } catch (Throwable ex){
            LOGGER.error("disconnect error", ex);
        }
    }

    public void close(){
        this.connectionManager.close();
        this.timer.stop();
        if(workGroup != null){
            workGroup.shutdownGracefully();
        }
    }
}
