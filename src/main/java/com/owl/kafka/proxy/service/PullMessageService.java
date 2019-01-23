package com.owl.kafka.proxy.service;

import com.owl.kafka.proxy.ClientConfigs;
import com.owl.kafka.proxy.DefaultPullMessageImpl;
import com.owl.kafka.proxy.transport.Address;
import com.owl.kafka.proxy.transport.NettyClient;
import com.owl.kafka.proxy.transport.Reconnector;
import com.owl.kafka.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.proxy.transport.protocol.Packet;
import com.owl.kafka.proxy.util.Packets;
import com.owl.kafka.util.NamedThreadFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @Author: Tboy
 */
public class PullMessageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPullMessageImpl.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("scheduled-pull-message-service"));

    private final NettyClient nettyClient;

    private final int pullTimeoutMs = 30 * 1000;

    private final int processQueueSize = ClientConfigs.I.getProcessQueueSize();

    public PullMessageService(NettyClient nettyClient){
        this.nettyClient = nettyClient;
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                InvokerPromise.scan();
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    public void pull(){
        if(ProcessQueue.I.getMessageCount() > 1000){
            for(Address address : nettyClient.getReconnectors().keySet()){
                pullLater(address);
            }
        } else{
            for(Address address : nettyClient.getReconnectors().keySet()){
                pullImmediately(address);
            }
        }
    }

    public void pullImmediately(Address address){
        if(ProcessQueue.I.getMessageCount() > processQueueSize){
            LOGGER.error("flow control, pull later : {} for process queue count : {} , more than config  : {}",
                    new Object[]{address, ProcessQueue.I.getMessageCount(), processQueueSize});
            pullLater(address);
            return;
        }
        Reconnector reconnector =  nettyClient.getReconnectors().get(address);
        if(reconnector == null){
            LOGGER.error("no Reconnector for : {}", address);
            return;
        }
        long opaque = IdService.I.getId();
        PullCallback callback = new PullCallback(){

            @Override
            public void onComplete(Packet packet) {
                pullImmediately(address);
            }

            @Override
            public void onException(Throwable ex) {
                LOGGER.error("exception on pull ", ex);
                pullLater(address);
            }
        };
        try {
            reconnector.getConnection().send(Packets.pullReq(opaque), new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    Throwable ex = future.cause();
                    new InvokerPromise(opaque, pullTimeoutMs, new InvokeCallback() {
                        @Override
                        public void onComplete(InvokerPromise invokerPromise) {
                            Packet response = invokerPromise.getResult();
                            if(response != null){
                                callback.onComplete(response);
                            } else if(invokerPromise.isTimeout()){
                                callback.onException(new TimeoutException("timeout exception"));
                            } else{
                                callback.onException(ex == null ? new Exception("unknown exception") : ex);
                            }
                        }
                    });
                }
            });
        } catch (ChannelInactiveException ex) {
            LOGGER.error("ChannelInactiveException", ex);
            pullLater(address);
        }
    }

    public void pullLater(Address address){
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                pullImmediately(address);
            }
        }, 3000, TimeUnit.MILLISECONDS);
    }

    public void close(){
        scheduledExecutorService.shutdown();
    }
}
