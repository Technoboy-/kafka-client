package com.owl.kafka.client.util;

import com.owl.kafka.client.service.IdService;
import com.owl.kafka.client.transport.protocol.Command;
import com.owl.kafka.client.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public class Packets {

    private static byte[] EMPTY_HEADER = new byte[0];

    private static byte[] EMPTY_KEY = new byte[0];

    private static byte[] EMPTY_VALUE = new byte[0];

    public static Packet ping(){
        Packet ping = new Packet();
        ping.setMsgId(IdService.I.getId());
        ping.setCmd(Command.PING.getCmd());
        ping.setHeader(EMPTY_HEADER);
        ping.setKey(EMPTY_KEY);
        ping.setValue(EMPTY_VALUE);
        return ping;
    }

    public static Packet pong(){
        Packet pong = new Packet();
        pong.setMsgId(IdService.I.getId());
        pong.setCmd(Command.PONG.getCmd());
        pong.setHeader(EMPTY_HEADER);
        pong.setKey(EMPTY_KEY);
        pong.setValue(EMPTY_VALUE);
        return pong;
    }

    public static Packet unregister(){
        Packet unregister = new Packet();
        unregister.setMsgId(IdService.I.getId());
        unregister.setCmd(Command.UNREGISTER.getCmd());
        unregister.setHeader(new byte[0]);
        unregister.setKey(EMPTY_KEY);
        unregister.setValue(EMPTY_VALUE);
        return unregister;
    }

    public static Packet ack(long msgId){
        Packet ack = new Packet();
        ack.setCmd(Command.ACK.getCmd());
        ack.setMsgId(msgId);
        ack.setHeader(EMPTY_HEADER);
        ack.setKey(EMPTY_KEY);
        ack.setValue(EMPTY_VALUE);
        return ack;
    }
}
