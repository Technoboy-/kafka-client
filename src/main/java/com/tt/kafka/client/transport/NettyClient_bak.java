package com.tt.kafka.client.transport;

import com.tt.kafka.client.service.LoadBalance;
import com.tt.kafka.client.service.RegisterMetadata;
import com.tt.kafka.client.service.RegistryService;
import com.tt.kafka.client.service.RoundRobinLoadBalance;
import com.tt.kafka.client.transport.codec.PacketDecoder;
import com.tt.kafka.client.transport.codec.PacketEncoder;
import com.tt.kafka.client.transport.handler.ClientHandler;
import com.tt.kafka.client.transport.handler.MessageDispatcher;
import com.tt.kafka.client.transport.handler.PushMessageHandler;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;
import com.tt.kafka.util.NamedThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class NettyClient_bak {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient_bak.class);

    private Bootstrap bootstrap;

    private NioEventLoopGroup workGroup;

    private volatile Channel channel = null;

    private final MessageListenerService messageListenerService;

    private final RegistryService registryService;

    private final ScheduledThreadPoolExecutor reconnectService;

    private ScheduledFuture<?> scheduledFuture;

    private final LoadBalance<Address> loadBalance = new RoundRobinLoadBalance();

    public NettyClient_bak(RegistryService registryService, MessageListenerService messageListenerService){
        this.registryService = registryService;
        this.messageListenerService = messageListenerService;
        this.reconnectService = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("reconnect-thread"));
        init();
    }

    private void init() {
        //
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.PUSH, new PushMessageHandler(messageListenerService));
        ClientHandler handler = new ClientHandler(dispatcher);

        bootstrap = new Bootstrap();
        workGroup = new NioEventLoopGroup(registryService.getClientConfigs().getClientWorkerNum());
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

    public void connect() {
        if (isConnected()) {
            return;
        }
        doConnect();
        scheduledFuture = reconnectService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (!isConnected()) {
                    doConnect();
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private void afterConnect(){
        if(channel != null && channel.localAddress() instanceof InetSocketAddress){
            InetSocketAddress address = (InetSocketAddress)channel.localAddress();
            RegisterMetadata<NettyClient_bak> metadata = new RegisterMetadata();
            metadata.setPath(String.format(Constants.ZOOKEEPER_CONSUMERS, registryService.getClientConfigs().getClientTopic()));
            Address client = new Address(address.getHostName(), address.getPort());
            metadata.setAddress(client);
            metadata.setRef(this);
            registryService.register(metadata);
        }
    }

    private void destroy(Channel oldChannel){
        if(oldChannel != null && oldChannel.localAddress() instanceof InetSocketAddress){
            InetSocketAddress address = (InetSocketAddress)oldChannel.localAddress();
            RegisterMetadata<NettyClient_bak> metadata = new RegisterMetadata();
            metadata.setPath(String.format(Constants.ZOOKEEPER_CONSUMERS, registryService.getClientConfigs().getClientTopic()));
            Address client = new Address(address.getHostName(), address.getPort());
            metadata.setAddress(client);
            registryService.destroy(metadata);
        }
        if(oldChannel != null){
            oldChannel.close();
        }
    }

    public void disconnect(){
        try {
            if(scheduledFuture != null && !scheduledFuture.isDone()){
                scheduledFuture.cancel(true);
                this.reconnectService.purge();
            }
            if(this.channel != null){
                this.channel.close();
            }
        } catch (Throwable ex){
            LOGGER.error("disconnect error", ex);
        }
    }

    private void doConnect() {
        ChannelFuture future = null;
        try {
            Address provider = loadBalance.select(registryService.getCopyProviders());
            if(provider == null){
                return;
            }
            InetSocketAddress address = new InetSocketAddress(provider.getHost(), provider.getPort());
            future = bootstrap.connect(address);
            boolean connected = future.awaitUninterruptibly(3000, TimeUnit.MILLISECONDS);
            if (connected && future.isSuccess()) {
                Channel newChannel = future.channel();
                try {
                    Channel oldChannel = channel;
                    destroy(oldChannel);
                } finally {
                    channel = newChannel;
                }
                LOGGER.info("connect to server : {} success", address);
            } else if (future.cause() != null) {
                LOGGER.error("connect to server " + address + " fail", future.cause());
            } else {
                LOGGER.error("connect to server " + address + " fail", future.cause());
            }
        } finally {
            if(isConnected()){
                afterConnect();
            }
            if (!isConnected() && future != null) {
                future.cancel(true);
            }
        }
    }

    private boolean isConnected() {
        if (channel != null) {
            return channel.isActive();
        }
        return false;
    }

    public void close(){
        disconnect();
        if(reconnectService != null){
            reconnectService.shutdown();
        }
        if(workGroup != null){
            workGroup.shutdownGracefully();
        }
    }

}
