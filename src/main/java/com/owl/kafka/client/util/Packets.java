package com.owl.kafka.client.util;

import com.owl.kafka.client.service.IdService;
import com.owl.kafka.client.service.PullStatus;
import com.owl.kafka.client.transport.message.Message;
import com.owl.kafka.client.transport.protocol.Command;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.serializer.SerializerImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.zookeeper.data.Id;

import java.nio.ByteBuffer;

/**
 * @Author: Tboy
 */
public class Packets {

    private static byte[] EMPTY_BODY = new byte[0];

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
        buf.writeBytes(EMPTY_BODY);
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
        ping.setBody(EMPTY_BODY);
        return ping;
    }

    public static Packet pong(){
        Packet pong = new Packet();
        pong.setOpaque(0);
        pong.setCmd(Command.PONG.getCmd());
        pong.setBody(EMPTY_BODY);
        return pong;
    }

    public static Packet unregister(){
        Packet unregister = new Packet();
        unregister.setOpaque(0);
        unregister.setCmd(Command.UNREGISTER.getCmd());
        unregister.setBody(EMPTY_BODY);
        return unregister;
    }

    public static Packet ack(long msgId){
        Packet ack = new Packet();
        ack.setCmd(Command.ACK.getCmd());
        ack.setOpaque(IdService.I.getId());

        Header header = new Header(msgId);
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        ByteBuffer buffer = ByteBuffer.allocate(4 + headerInBytes.length + 4 + 4);
        buffer.putInt(headerInBytes.length);
        buffer.put(headerInBytes);
        buffer.putInt(0);
        buffer.put(EMPTY_KEY);
        buffer.putInt(0);
        buffer.put(EMPTY_VALUE);
        ack.setBody(buffer.array());

        return ack;
    }

    public static Packet view(long msgId){
        Packet ack = new Packet();
//        ack.setCmd(Command.VIEW.getCmd());
//        ack.setOpaque(IdService.I.getId());
//
//        Header header = new Header(msgId);
//        ack.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
//        ack.setKey(EMPTY_KEY);
//        ack.setValue(EMPTY_VALUE);
        return ack;
    }

    public static Packet pull(long opaque){
        Packet pull = new Packet();
//        //
        pull.setCmd(Command.PULL_REQ.getCmd());
        pull.setOpaque(opaque);
        pull.setBody(EMPTY_BODY);

        return pull;
    }

    public static Packet toViewPacket(long msgId, Record<byte[], byte[]> record){
        Packet packet = new Packet();
//        //
//        packet.setCmd(Command.VIEW.getCmd());
//        packet.setOpaque(msgId);
//        Header header = new Header(record.getTopic(), record.getPartition(), record.getOffset());
//
//        packet.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
//        packet.setKey(record.getKey());
//        packet.setValue(record.getValue());

        return packet;
    }

    public static Packet toPullPacket(ConsumerRecord<byte[], byte[]> record){
        Packet packet = new Packet();
//        //
//        packet.setCmd(Command.PULL.getCmd());
//        packet.setOpaque(IdService.I.getId());
//        Header header = new Header(record.topic(), record.partition(), record.offset());
//        packet.setHeader(SerializerImpl.getFastJsonSerializer().serialize(header));
//        packet.setKey(record.key());
//        packet.setValue(record.value());

        return packet;
    }

    public static Packet toSendBackPacket(Message message){
        Packet back = new Packet();
        //
        back.setCmd(Command.SEND_BACK.getCmd());
        back.setOpaque(IdService.I.getId());
        //
        ByteBuffer buffer = ByteBuffer.allocate(message.getHeaderInBytes().length + 4 + 4 + 4);
        buffer.putInt(message.getHeaderInBytes().length);
        buffer.put(message.getHeaderInBytes());
        buffer.putInt(0);
        buffer.put(EMPTY_KEY);
        buffer.putInt(0);
        buffer.put(EMPTY_VALUE);
        back.setBody(buffer.array());

        return back;
    }

    public static Packet noNewMsg(long opaque){
        Packet result = new Packet();
        //
        result.setOpaque(opaque);
        result.setCmd(Command.PULL_RESP.getCmd());
        //
        Header header = new Header(PullStatus.NO_NEW_MSG.getStatus());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        ByteBuffer buffer = ByteBuffer.allocate(headerInBytes.length + 4 + 4 + 4);
        buffer.putInt(headerInBytes.length);
        buffer.put(headerInBytes);
        buffer.putInt(0);
        buffer.put(EMPTY_KEY);
        buffer.putInt(0);
        buffer.put(EMPTY_VALUE);
        result.setBody(buffer.array());

        return result;
    }

    public static Packet pullResp(long opaque){
        Packet pull = new Packet();
        pull.setOpaque(opaque);
        pull.setCmd(Command.PULL_RESP.getCmd());
        pull.setBody(EMPTY_BODY);
        return pull;
    }
}
