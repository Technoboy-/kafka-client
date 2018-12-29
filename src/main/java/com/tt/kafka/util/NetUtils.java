package com.tt.kafka.util;

import io.netty.channel.Channel;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * @Author: Tboy
 */
public class NetUtils {

    public static String getLocalIp(){
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    public static String getLocalAddress(Channel channel){
        return ((InetSocketAddress)channel.localAddress()).getAddress().getHostAddress();
    }

    public static String getRemoteAddress(Channel channel){
        return ((InetSocketAddress)channel.remoteAddress()).getAddress().getHostAddress();
    }

    public static void main(String[] args) {
        System.out.println(getLocalIp());
    }
}
