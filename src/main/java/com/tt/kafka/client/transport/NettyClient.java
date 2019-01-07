package com.tt.kafka.client.transport;

import com.tt.kafka.client.service.*;
import com.tt.kafka.client.transport.handler.ClientHandler;
import com.tt.kafka.client.transport.handler.MessageDispatcher;
import com.tt.kafka.client.transport.handler.PushMessageHandler;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;
import com.tt.kafka.util.NamedThreadFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class NettyClient extends Transporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private final LoadBalance<Address> loadBalance = new RoundRobinLoadBalance();

    private final ScheduledThreadPoolExecutor executorService;

    private ScheduledFuture<?> reconnectScheduledFuture;

    private ScheduledFuture<?> heartbeatScheduledFuture;

    private final RegistryService registryService;

    private volatile Channel channel;

    private InetSocketAddress lastAddress;

    private final MessageDispatcher dispatcher;

    private final ClientHandler clientHandler;

    public NettyClient(RegistryService registryService, MessageListenerService messageListenerService){
        super(registryService.getClientConfigs().getClientWorkerNum());
        this.registryService = registryService;
        this.dispatcher = new MessageDispatcher();
        this.dispatcher.register(Command.PUSH, new PushMessageHandler(messageListenerService));
        this.clientHandler = new ClientHandler(this.dispatcher);
        this.executorService = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("reconnect-hearbeat-thread"));
        initHandler(clientHandler);
    }

    public void connect(InetSocketAddress address) {
        this.lastAddress = address;
        this.doConnect(address);
        this.startSchedulerTask();
    }

    private void startSchedulerTask(){
        reconnectScheduledFuture = executorService.scheduleAtFixedRate(new Runnable() {
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

        heartbeatScheduledFuture = executorService.scheduleAtFixedRate(new Runnable() {
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

    public void disconnect(){
        try {
            if(reconnectScheduledFuture != null && !reconnectScheduledFuture.isDone()){
                reconnectScheduledFuture.cancel(true);
            }
            if(heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()){
                heartbeatScheduledFuture.cancel(true);
            }
            this.executorService.purge();
            unregisterClient();
            this.destroy(channel);
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

    private void afterConnect(){
        if(channel != null && channel.localAddress() instanceof InetSocketAddress){
            InetSocketAddress address = (InetSocketAddress)channel.localAddress();
            RegisterMetadata<Transporter> metadata = new RegisterMetadata();
            metadata.setPath(String.format(Constants.ZOOKEEPER_CONSUMERS, registryService.getClientConfigs().getClientTopic()));
            Address client = new Address(address.getHostName(), address.getPort());
            metadata.setAddress(client);
            metadata.setRef(this);
            registryService.register(metadata);
        }
    }

    protected void destroy(Channel oldChannel){
        if(oldChannel != null && oldChannel.localAddress() instanceof InetSocketAddress){
            InetSocketAddress address = (InetSocketAddress)oldChannel.localAddress();
            RegisterMetadata<Transporter> metadata = new RegisterMetadata();
            metadata.setPath(String.format(Constants.ZOOKEEPER_CONSUMERS, registryService.getClientConfigs().getClientTopic()));
            Address client = new Address(address.getHostName(), address.getPort());
            metadata.setAddress(client);
            registryService.unregister(metadata);
        }
        if(oldChannel != null){
            oldChannel.close();
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
        super.close();
    }
}
