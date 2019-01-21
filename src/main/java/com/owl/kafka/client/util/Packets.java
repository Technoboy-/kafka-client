package com.owl.kafka.client.util;

import com.owl.kafka.client.service.IdService;
import com.owl.kafka.client.service.PullStatus;
import com.owl.kafka.client.transport.protocol.Command;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.serializer.SerializerImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.zookeeper.data.Id;

/**
 * @Author: Tboy
 */
public class Packets {

    private static byte[] EMPTY_HEADER = new byte[0];

    private static byte[] EMPTY_KEY = new byte[0];

    private static byte[] EMPTY_VALUE = new byte[0];

    private static final ByteBuf HEARTBEATS_BUF;

    static {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(Packet.MAGIC);
        buf.writeByte(Packet.VERSION);
        buf.writeByte(Command.PING.getCmd());
        buf.writeLong(0);
        buf.writeInt(0);
        buf.writeBytes(EMPTY_HEADER);
        buf.writeInt(0);
        buf.writeBytes(EMPTY_KEY);
        buf.writeInt(0);
        buf.writeBytes(EMPTY_VALUE);
        HEARTBEATS_BUF = Unpooled.unreleasableBuffer(buf).asReadOnly();
    }

    public static ByteBuf heartbeatContent(){
        return HEARTBEATS_BUF.duplicate();
    }

    public static ByteBuf registerContent(){
        return HEARTBEATS_BUF.duplicate();
    }

    public static Packet ping(){
        Packet ping = new Packet();
        ping.setOpaque(0);
        ping.setCmd(Command.PING.getCmd());
        ping.setHeader(EMPTY_HEADER);
        ping.setKey(EMPTY_KEY);
        ping.setValue(EMPTY_VALUE);
        return ping;
    }

    public static Packet pong(){
        Packet pong = new Packet();
        pong.setOpaque(0);
        pong.setCmd(Command.PONG.getCmd());
        pong.setHeader(EMPTY_HEADER);
        pong.setKey(EMPTY_KEY);
        pong.setValue(EMPTY_VALUE);
        return pong;
    }

    public static Packet unregister(){
        Packet unregister = new Packet();
        unregister.setOpaque(0);
        unregister.setCmd(Command.UNREGISTER.getCmd());
        unregister.setHeader(new byte[0]);
        unregister.setKey(EMPTY_KEY);
        unregister.setValue(EMPTY_VALUE);
        return unregister;
    }

    public static Packet ack(long msgId){
        Packet ack = new Packet();
        ack.setCmd(Command.ACK.getCmd());
        ack.setOpaque(msgId);
        ack.setHeader(EMPTY_HEADER);
        ack.setKey(EMPTY_KEY);
        ack.setValue(EMPTY_VALUE);
        return ack;
    }

    public static Packet view(long msgId){
        Packet ack = new Packet();
        ack.setCmd(Command.VIEW.getCmd());
        ack.setOpaque(msgId);
        ack.setHeader(EMPTY_HEADER);
        ack.setKey(EMPTY_KEY);
        ack.setValue(EMPTY_VALUE);
        return ack;
    }

    public static Packet pull(long msgId){
        Packet pull = new Packet();
        //
        pull.setCmd(Command.PULL.getCmd());
        pull.setOpaque(msgId);
        pull.setHeader(EMPTY_HEADER);
        pull.setKey(EMPTY_KEY);
        pull.setValue(EMPTY_VALUE);

        return pull;
    }

    public static Packet toViewPacket(long msgId, Record<byte[], byte[]> record){
        Packet packet = new Packet();
        //
        packet.setCmd(Command.VIEW.getCmd());
        packet.setOpaque(msgId);
        Header header = new Header(record.getTopic(), record.getPartition(), record.getOffset());
        packet.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
        packet.setKey(record.getKey());
        packet.setValue(record.getValue());

        return packet;
    }

    public static Packet toPullPacket(ConsumerRecord<byte[], byte[]> record){
        Packet packet = new Packet();
        //
        packet.setCmd(Command.PULL.getCmd());
        packet.setOpaque(IdService.I.getId());
        Header header = new Header(record.topic(), record.partition(), record.offset());
        packet.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
        packet.setKey(record.key());
        packet.setValue(record.value());

        return packet;
    }

    public static Packet toSendBackPacket(Packet packet){
        Packet back = new Packet();
        //
        back.setCmd(Command.SEND_BACK.getCmd());
        back.setOpaque(packet.getOpaque());

        return packet;
    }

    public static Packet noNewMsg(long msgId){
        Packet ping = new Packet();
        ping.setOpaque(msgId);
        ping.setCmd(Command.PULL.getCmd());
        Header header = new Header(PullStatus.NO_NEW_MSG.getStatus());
        ping.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
        ping.setKey(EMPTY_KEY);
        ping.setValue(EMPTY_VALUE);
        return ping;
    }
}
