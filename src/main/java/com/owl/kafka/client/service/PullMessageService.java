package com.owl.kafka.client.service;

import com.owl.kafka.client.DefaultPullMessageImpl;
import com.owl.kafka.client.transport.Address;
import com.owl.kafka.client.transport.NettyClient;
import com.owl.kafka.client.transport.Reconnector;
import com.owl.kafka.client.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
public class PullMessageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPullMessageImpl.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("scheduled-pull-message-service"));

    private NettyClient nettyClient;

    public void setNettyClient(NettyClient nettyClient) {
         this.nettyClient = nettyClient;
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
        Reconnector reconnector =  nettyClient.getReconnectors().get(address);
        try {
            reconnector.getConnection().send(Packets.pull());
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
        }, 3, TimeUnit.SECONDS);
    }

    public void close(){
        scheduledExecutorService.shutdown();
    }
}
