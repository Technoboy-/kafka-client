package com.owl.kafka.client.util;

import com.owl.kafka.client.service.IdService;
import com.owl.kafka.client.transport.protocol.Command;
import com.owl.kafka.client.transport.protocol.Packet;

/**
 * @Author: Tboy
 */
public class Unregisters {

    public static Packet unregister(){
        Packet unregister = new Packet();
        unregister.setMsgId(IdService.I.getId());
        unregister.setCmd(Command.UNREGISTER.getCmd());
        unregister.setHeader(new byte[0]);
        unregister.setKey(new byte[0]);
        unregister.setValue(new byte[0]);
        return unregister;
    }
}
