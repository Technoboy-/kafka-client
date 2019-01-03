package com.tt.kafka.client.netty.service;

import com.tt.kafka.client.netty.protocol.Command;
import com.tt.kafka.client.netty.protocol.Packet;
import com.tt.kafka.client.netty.transport.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
public class HeartbeatService{

    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatService.class);

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final Connection connection;

    private final IdService idService;

    private final AtomicBoolean start = new AtomicBoolean(false);

    public HeartbeatService(Connection connection){
        this.connection = connection;
        this.idService = new IdService();
    }

    public void start(){
        if(start.compareAndSet(false, true)){
            executorService.scheduleAtFixedRate(new HeartbeatTask(), 10, 100, TimeUnit.SECONDS);
        }
    }

    public void close(){
        start.compareAndSet(true, false);
        executorService.shutdown();
    }

    private Packet createPacket(){
        Packet packet = new Packet();
        packet.setMsgId(idService.getId());
        packet.setCmd(Command.HEARTBEAT.getCmd());
        packet.setHeader(new byte[0]);
        packet.setKey(new byte[0]);
        packet.setValue(new byte[0]);
        return packet;
    }

    class HeartbeatTask implements Runnable{

        @Override
        public void run() {
            try {
                connection.send(createPacket());
            } catch (Exception ex){
                LOGGER.error("HeartbeatTask error {}, close channel ", ex);
                connection.close();
            }
        }
    }
}
