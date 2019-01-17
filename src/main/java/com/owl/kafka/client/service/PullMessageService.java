package com.owl.kafka.client.service;

import com.owl.kafka.client.transport.Address;
import com.owl.kafka.client.transport.NettyClient;
import com.owl.kafka.client.transport.Reconnector;
import com.owl.kafka.client.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.util.Packets;

/**
 * @Author: Tboy
 */
public class PullMessageService {



    private final NettyClient nettyClient;

    public PullMessageService(NettyClient nettyClient){
        this.nettyClient = nettyClient;
    }


    public void pull(Address address){
        Reconnector reconnector =  nettyClient.getReconnectors().get(address);

        try {
            reconnector.getConnection().send(Packets.pull());
        } catch (ChannelInactiveException e) {

        }
    }
}
