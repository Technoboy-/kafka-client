package com.tt.kafka.client.util;

import com.tt.kafka.client.service.IdService;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public class Heartbeats {

    public static Packet heartbeat(){
        Packet hearbeat = new Packet();
        hearbeat.setMsgId(IdService.I.getId());
        hearbeat.setCmd(Command.HEARTBEAT.getCmd());
        hearbeat.setHeader(new byte[0]);
        hearbeat.setKey(new byte[0]);
        hearbeat.setValue(new byte[0]);
        return hearbeat;
    }
}
