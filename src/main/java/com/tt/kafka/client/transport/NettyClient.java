package com.tt.kafka.client.transport;

import com.tt.kafka.client.service.*;
import com.tt.kafka.client.transport.codec.PacketDecoder;
import com.tt.kafka.client.transport.codec.PacketEncoder;
import com.tt.kafka.client.transport.handler.ClientHandler;
import com.tt.kafka.client.transport.handler.MessageDispatcher;
import com.tt.kafka.client.transport.handler.PushMessageHandler;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;
import com.tt.kafka.util.NamedThreadFactory;
import com.tt.kafka.util.Pair;
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
public class NettyClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private Bootstrap bootstrap;

    private NioEventLoopGroup workGroup;

    private volatile Channel channel = null;

    private final Pair<MessageListener, MessageListenerService> pair;

    private final RegistryService registryService;

    private final ScheduledThreadPoolExecutor reconnectService;

    private final ScheduledThreadPoolExecutor heartbeatService;

    private ScheduledFuture<?> reconnectScheduledFuture;

    private ScheduledFuture<?> heartbeatScheduledFuture;

    private final LoadBalance<Address> loadBalance = new RoundRobinLoadBalance();

    private InetSocketAddress lastAddress;

    public NettyClient(RegistryService registryService, Pair<MessageListener, MessageListenerService> pair){
        this.registryService = registryService;
        this.pair = pair;
        this.reconnectService = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("reconnect-thread"));
        this.heartbeatService = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("heartbeat-thread"));
        init();
    }

    private void init() {
        //
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.PUSH, new PushMessageHandler(pair.getL(), pair.getR()));
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

    public void connect(InetSocketAddress address) {
        lastAddress = address;
        if (isConnected()) {
            return;
        }
        doConnect(address);
        startSchedulerTask();
    }

    private void startSchedulerTask(){
        reconnectScheduledFuture = reconnectService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (!isConnected()) {
                    boolean connect = doConnect(lastAddress);
                    if(!connect){
                        Address provider = loadBalance.select(registryService.getCopyProviders());
                        if(provider != null){
                            InetSocketAddress newAddress = new InetSocketAddress(provider.getHost(), provider.getPort());
                            if(!newAddress.equals(lastAddress)){
                                lastAddress = newAddress;
                                doConnect(lastAddress);
                            }
                        }
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS);

        heartbeatScheduledFuture = heartbeatService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (isConnected()) {
                    Packet packet = new Packet();
                    packet.setMsgId(IdService.I.getId());
                    packet.setCmd(Command.HEARTBEAT.getCmd());
                    packet.setHeader(new byte[0]);
                    packet.setKey(new byte[0]);
                    packet.setValue(new byte[0]);
                    channel.writeAndFlush(packet);
                }
            }
        }, 1, 20, TimeUnit.SECONDS);
    }

    private void afterConnect(){
        if(channel != null && channel.localAddress() instanceof InetSocketAddress){
            InetSocketAddress address = (InetSocketAddress)channel.localAddress();
            RegisterMetadata<NettyClient> metadata = new RegisterMetadata();
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
            RegisterMetadata<NettyClient> metadata = new RegisterMetadata();
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
            if(reconnectScheduledFuture != null && !reconnectScheduledFuture.isDone()){
                reconnectScheduledFuture.cancel(true);
                this.reconnectService.purge();
            }
            if(heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()){
                heartbeatScheduledFuture.cancel(true);
                this.heartbeatService.purge();
            }
            unregisterClient();
            if(this.channel != null){
                this.channel.close();
            }
        } catch (Throwable ex){
            LOGGER.error("disconnect error", ex);
        }
    }

    private void unregisterClient(){
        Packet packet = new Packet();
        packet.setMsgId(IdService.I.getId());
        packet.setCmd(Command.UNREGISTER.getCmd());
        packet.setHeader(new byte[0]);
        packet.setKey(new byte[0]);
        packet.setValue(new byte[0]);
        this.channel.writeAndFlush(packet);
    }

    private boolean doConnect(InetSocketAddress address) {
        ChannelFuture future = null;
        try {
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
            return isConnected();
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
        this.disconnect();
        if(reconnectService != null){
            reconnectService.shutdown();
        }
        if(workGroup != null){
            workGroup.shutdownGracefully();
        }
    }

}
