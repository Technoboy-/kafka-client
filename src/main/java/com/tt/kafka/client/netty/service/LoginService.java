package com.tt.kafka.client.netty.service;

import com.tt.kafka.client.netty.protocol.Command;
import com.tt.kafka.client.netty.protocol.Packet;
import com.tt.kafka.client.netty.transport.Connection;

/**
 * @Author: Tboy
 */
public class LoginService{

    private final IdService idService;

    private final Connection connection;

    public LoginService(Connection connection){
        this.connection = connection;
        this.idService = new IdService();
    }

    private Packet createPacket(){
        Packet packet = new Packet();
        packet.setMsgId(idService.getId());
        packet.setCmd(Command.LOGIN.getCmd());
        packet.setHeader(new byte[0]);
        packet.setKey(new byte[0]);
        packet.setValue(new byte[0]);
        return packet;
    }

    public void login() throws Exception {
        this.connection.send(createPacket());
    }
}
