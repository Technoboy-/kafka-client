package com.tt.kafka.client.transport;

import com.tt.kafka.client.util.SystemPropertiesUtils;
import com.tt.kafka.client.transport.codec.PacketDecoder;
import com.tt.kafka.client.transport.codec.PacketEncoder;
import com.tt.kafka.client.transport.handler.*;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;
import com.tt.kafka.util.NamedThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
public class NettyConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyConnector.class);

    private final IdleStateTrigger idleStateTrigger = new IdleStateTrigger();

    private final PacketEncoder encoder = new PacketEncoder();

    private final HashedWheelTimer timer = new HashedWheelTimer(new NamedThreadFactory("connector.timer"));

    private final ConcurrentHashMap<InetSocketAddress, Reconnector> reconnectors = new ConcurrentHashMap<>();

    private Bootstrap bootstrap = new Bootstrap();

    private final int workerNum = SystemPropertiesUtils.getInt(Constants.PUSH_CLIENT_WORKER_NUM, Constants.CPU_SIZE);

    private final NioEventLoopGroup workGroup = new NioEventLoopGroup(workerNum);

    private final MessageDispatcher dispatcher = new MessageDispatcher();

    private ClientHandler handler;

    public NettyConnector(MessageListenerService messageListenerService){
        bootstrap.
                option(ChannelOption.SO_KEEPALIVE, true).
                option(ChannelOption.TCP_NODELAY, true).
                group(workGroup).
                channel(NioSocketChannel.class);
        initHandler(messageListenerService);
    }

    private void initHandler(MessageListenerService messageListenerService){
        this.dispatcher.register(Command.PUSH, new PushMessageHandler(messageListenerService));
        this.handler = new ClientHandler(this.dispatcher);
    }

    public void connect(InetSocketAddress address) {
        //
        final ConnectionWatchDog connectionWatchDog = new ConnectionWatchDog(bootstrap, timer, address){

            @Override
            public ChannelHandler[] handlers() {
                return new ChannelHandler[]{this,
                        new IdleStateHandler(60, 60, 9),
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
                future = bootstrap.connect(address);
            }
            reconnectors.put(address, new Reconnector(connectionWatchDog, future));
        } catch (Throwable ex){
            throw new RuntimeException("Connects to [" + address + "] fails", ex);
        }
    }

    public void disconnect(InetSocketAddress address){
        try {
            Reconnector reconnector = reconnectors.remove(address);
            if(reconnector != null){
                reconnector.setReconnect(false);
            }
        } catch (Throwable ex){
            LOGGER.error("disconnect error", ex);
        }
    }

    public void close(){
        for(InetSocketAddress address : reconnectors.keySet()){
            disconnect(address);
        }
        if(workGroup != null){
            workGroup.shutdownGracefully();
        }
    }
}
