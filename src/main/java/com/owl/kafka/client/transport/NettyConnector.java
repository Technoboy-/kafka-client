package com.owl.kafka.client.transport;

import com.owl.kafka.client.transport.codec.PacketDecoder;
import com.owl.kafka.client.transport.codec.PacketEncoder;
import com.owl.kafka.client.transport.handler.*;
import com.owl.kafka.client.ClientConfigs;
import com.owl.kafka.client.transport.protocol.Command;
import com.owl.kafka.consumer.service.MessageListenerService;
import com.owl.kafka.util.NamedThreadFactory;
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

    private final int workerNum = ClientConfigs.I.getWorkerNum();

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
        this.dispatcher.register(Command.PONG, new PongMessageHandler());
        this.dispatcher.register(Command.PUSH, new PushMessageHandler(messageListenerService));
        this.handler = new ClientHandler(this.dispatcher);
    }

    public void connect(InetSocketAddress address, boolean isSync) {
        //
        final ConnectionWatchDog connectionWatchDog = new ConnectionWatchDog(bootstrap, timer, address){

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
                future = bootstrap.connect(address);
            }
            if(isSync){
               future.sync();
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
                reconnector.disconnect();
            }
        } catch (Throwable ex){
            LOGGER.error("disconnect error", ex);
        }
    }

    public void close(){
        for(Reconnector reconnector : reconnectors.values()){
            reconnector.close();
        }
        if(workGroup != null){
            workGroup.shutdownGracefully();
        }
    }
}
