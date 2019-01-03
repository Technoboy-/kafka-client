package com.tt.kafka.client.netty.transport;

import com.tt.kafka.client.service.Address;
import com.tt.kafka.client.service.RegisterMetadata;
import com.tt.kafka.client.service.RegistryService;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.client.netty.codec.PacketDecoder;
import com.tt.kafka.client.netty.codec.PacketEncoder;
import com.tt.kafka.client.netty.handler.*;
import com.tt.kafka.client.netty.protocol.Command;
import com.tt.kafka.util.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class PushTcpClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushTcpClient.class);

    private Bootstrap bootstrap;

    private NioEventLoopGroup workGroup;

    private String ip;

    private int port;

    protected Channel channel = null;

    private PushClientHandler handler;

    private MessageListenerService messageListenerService;

    private final RegistryService registryService;

    public PushTcpClient(RegistryService registryService, MessageListenerService messageListenerService){
        this.registryService = registryService;
        this.messageListenerService = messageListenerService;
        initHandler();
        initClient();
    }

    private void initHandler(){
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.register(Command.LOGIN, new LoginHandler());
        dispatcher.register(Command.HEARTBEAT_ACK, new HeartbeatHandler());
        dispatcher.register(Command.PUSH, new PushHandler(messageListenerService));
        handler = new PushClientHandler(dispatcher);
    }

    private void initClient() {
        bootstrap = new Bootstrap();
        int workNum = Runtime.getRuntime().availableProcessors() + 1;
        workGroup = new NioEventLoopGroup(workNum);
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

    public void connect(String ip, int port) {
        this.ip = ip;
        this.port = port;
        try {
            if (isConnected()) {
                return;
            }
            doConnect();
            if (!isConnected()) {
                throw new Exception("connect to server(ip:" + getIp() + ", port:" + getPort() + ") fail");
            }
        } catch (Throwable e) {
            LOGGER.error("connect to server(ip:" + getIp() + ", port:" + getIp() + ") fail", e);
            throw new RuntimeException(e);
        }
        LOGGER.info("connect to server(ip:" + getIp() + ", port:" + getPort() + ") success");
        afterConnect();
    }

    private void afterConnect(){
        if(channel != null && channel.localAddress() instanceof InetSocketAddress){
            InetSocketAddress address = (InetSocketAddress)channel.localAddress();
            RegisterMetadata<PushTcpClient> metadata = new RegisterMetadata();
            metadata.setPath(String.format(Constants.ZOOKEEPER_CONSUMERS, registryService.getClientConfigs().getClientTopic()));
            Address client = new Address(address.getHostName(), address.getPort());
            metadata.setAddress(client);
            metadata.setRef(this);
            registryService.register(metadata);
        }
    }

    private void doConnect() throws Throwable {
        ChannelFuture future = null;
        try {
            future = bootstrap.connect(getIp(), getPort());
            boolean connected = future.awaitUninterruptibly(5000, TimeUnit.MILLISECONDS);
            if (connected && future.isSuccess()) {
                Channel newChannel = future.channel();
                try {
                    Channel oldChannel = channel;
                    if (oldChannel != null) {
                        oldChannel.close();
                    }
                } finally {
                    channel = newChannel;
                }
            } else if (future.cause() != null) {
                throw new Exception(future.cause());
            } else {
                throw new Exception("connect " + "server(ip:" + ip + ", port:" + port + ") timeout");
            }
        } finally {
            if (!isConnected() && future != null) {
                future.cancel(true);
            }
        }
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    private boolean isConnected() {
        if (channel != null) {
            return channel.isActive();
        }
        return false;
    }

    public void close(){
        if(workGroup != null){
            workGroup.shutdownGracefully();
        }
    }

}
