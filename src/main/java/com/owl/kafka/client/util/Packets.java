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

import java.nio.ByteBuffer;

/**
 * @Author: Tboy
 */
public class Packets {

    private static byte[] EMPTY_BODY = new byte[0];

    private static byte[] EMPTY_KEY = new byte[0];

    private static byte[] EMPTY_VALUE = new byte[0];

    private static final ByteBuf PING_BUF;

    private static final ByteBuf PONE_BUF;

    static {
        ByteBuf ping = Unpooled.buffer();
        ping.writeByte(Packet.MAGIC);
        ping.writeByte(Packet.VERSION);
        ping.writeByte(Command.PING.getCmd());
        ping.writeLong(0);
        ping.writeInt(0);
        ping.writeBytes(EMPTY_BODY);
        PING_BUF = Unpooled.unreleasableBuffer(ping).asReadOnly();
        //
        ByteBuf pong = Unpooled.buffer();
        pong.writeByte(Packet.MAGIC);
        pong.writeByte(Packet.VERSION);
        pong.writeByte(Command.PONG.getCmd());
        pong.writeLong(0);
        pong.writeInt(0);
        pong.writeBytes(EMPTY_BODY);
        PONE_BUF = Unpooled.unreleasableBuffer(pong).asReadOnly();
    }

    public static ByteBuf pingContent(){
        return PING_BUF.duplicate();
    }

    public static ByteBuf registerContent(){
        return PING_BUF.duplicate();
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
        //
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

    public static Packet viewReq(long msgId){
        Packet ack = new Packet();
        ack.setCmd(Command.VIEW_REQ.getCmd());
        ack.setOpaque(IdService.I.getId());
        //
        Header header = new Header(msgId);
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        //
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

    public static Packet viewResp(long opaque, long msgId, Record<byte[], byte[]> record){
        Packet viewResp = new Packet();
        viewResp.setCmd(Command.VIEW_RESP.getCmd());
        viewResp.setOpaque(opaque);
        //
        Header header = new Header(record.getTopic(), record.getPartition(), record.getOffset(), msgId);
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);

        ByteBuffer buffer = ByteBuffer.allocate(4 + headerInBytes.length + 4 + record.getKey().length + 4 + record.getValue().length);
        buffer.putInt(headerInBytes.length);
        buffer.put(headerInBytes);
        buffer.putInt(record.getKey().length);
        buffer.put(record.getKey());
        buffer.putInt(record.getValue().length);
        buffer.put(record.getValue());

        viewResp.setBody(buffer.array());

        return viewResp;
    }

    public static Packet noViewMsgResp(long opaque){
        Packet viewResp = new Packet();
        viewResp.setCmd(Command.VIEW_RESP.getCmd());
        viewResp.setOpaque(opaque);
        viewResp.setBody(EMPTY_BODY);

        return viewResp;
    }

    public static Packet pullReq(long opaque){
        Packet pull = new Packet();

        pull.setCmd(Command.PULL_REQ.getCmd());
        pull.setOpaque(opaque);
        pull.setBody(EMPTY_BODY);

        return pull;
    }

    public static Packet sendBackReq(Message message){
        Packet back = new Packet();
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

    public static Packet noNewMsgResp(long opaque){
        Packet result = new Packet();
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

}
