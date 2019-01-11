package com.tt.kafka.client.transport;

import com.tt.kafka.client.SystemPropertiesUtils;
import com.tt.kafka.client.service.*;
import com.tt.kafka.client.transport.codec.PacketDecoder;
import com.tt.kafka.client.transport.codec.PacketEncoder;
import com.tt.kafka.client.transport.handler.ClientHandler;
import com.tt.kafka.client.transport.handler.MessageDispatcher;
import com.tt.kafka.client.transport.handler.PushMessageHandler;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;
import com.tt.kafka.util.NamedThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class NettyClient extends NettyConnector{

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    protected final HashedWheelTimer timer = new HashedWheelTimer(new NamedThreadFactory("connector.timer"));

    private final MessageDispatcher dispatcher = new MessageDispatcher();

    private final ClientHandler handler;

    public NettyClient(MessageListenerService messageListenerService){
        this.dispatcher.register(Command.PUSH, new PushMessageHandler(messageListenerService));
        this.handler = new ClientHandler(this.dispatcher);
    }
//
//        heartbeatScheduledFuture = executorService.scheduleAtFixedRate(new Runnable() {
//            @Override
//            public void run() {
//                Enumeration<InetSocketAddress> keys = channels.keys();
//                InetSocketAddress lastAddress = keys.nextElement();
//                Channel channel = channels.get(lastAddress);
//                while(keys.hasMoreElements()){
//                    if (isConnected(channel)) {
//                        Packet packet = new Packet();
//                        packet.setMsgId(IdService.I.getId());
//                        packet.setCmd(Command.HEARTBEAT.getCmd());
//                        packet.setHeader(new byte[0]);
//                        packet.setKey(new byte[0]);
//                        packet.setValue(new byte[0]);
//                        channel.writeAndFlush(packet);
//                    }
//                }
//            }
//        }, 1, 20, TimeUnit.SECONDS);
//    }

    public void connect(InetSocketAddress address) {
        ChannelFuture future = null;
        Channel channel = null;
        try {
            bootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
                protected void initChannel(NioSocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    //out
                    pipeline.addLast("encoder", new PacketEncoder());

                    //in
                    pipeline.addLast("decoder", new PacketDecoder());
                    pipeline.addLast("handler", new ConnectionWatchDog(bootstrap, timer, address){

                        @Override
                        ChannelHandler[] handlers() {
                            return new ChannelHandler[]{this, handler};
                        }
                    });
                }
            });
            future = bootstrap.connect(address);
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
                this.unregisterClient(channel);
                channel.close();
            }
        } catch (Throwable ex){
            LOGGER.error("disconnect error", ex);
        }
    }

    private void unregisterClient(Channel channel){
        Packet packet = new Packet();
        packet.setMsgId(IdService.I.getId());
        packet.setCmd(Command.UNREGISTER.getCmd());
        packet.setHeader(new byte[0]);
        packet.setKey(new byte[0]);
        packet.setValue(new byte[0]);
        channel.writeAndFlush(packet);
    }

    public void close(){
        for(InetSocketAddress address : channels.keySet()){
            disconnect(address);
        }
        super.close();
    }
}
