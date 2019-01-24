package com.owl.kafka.client.proxy.util;

import com.owl.kafka.client.proxy.service.IdService;
import com.owl.kafka.client.proxy.service.PullStatus;
import com.owl.kafka.client.proxy.service.TopicPartitionOffset;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.transport.protocol.Command;
import com.owl.kafka.client.proxy.transport.message.Header;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.consumer.Record;
import com.owl.kafka.client.serializer.SerializerImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
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

    private static final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    static {
        ByteBuf ping = Unpooled.buffer();
        ping.writeByte(Packet.MAGIC);
        ping.writeByte(Packet.VERSION);
        ping.writeByte(Command.PING.getCmd());
        ping.writeLong(0);
        ping.writeInt(0);
        ping.writeBytes(EMPTY_BODY);
        PING_BUF = Unpooled.unreleasableBuffer(ping).asReadOnly();
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

    public static Packet ackPushReq(long msgId){
        Packet ack = new Packet();
        ack.setCmd(Command.ACK.getCmd());
        ack.setOpaque(IdService.I.getId());

        Header header = new Header(msgId);
        header.setSign(Header.Sign.PUSH.getSign());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        //
        ByteBuf buffer = allocator.directBuffer(4 + headerInBytes.length + 4 + 4);
        try {
            buffer.writeInt(headerInBytes.length);
            buffer.writeBytes(headerInBytes);
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_KEY);
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_VALUE);
            //
            ack.setBody(buffer.array());
        } finally {
            buffer.release();
        }
        return ack;
    }

    public static Packet ackPullReq(TopicPartitionOffset topicPartitionOffset){
        Packet ack = new Packet();
        ack.setCmd(Command.ACK.getCmd());
        ack.setOpaque(IdService.I.getId());

        Header header = new Header(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition(), topicPartitionOffset.getOffset(), topicPartitionOffset.getMsgId());
        header.setSign(Header.Sign.PULL.getSign());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        //
        ByteBuf buffer = allocator.directBuffer(4 + headerInBytes.length + 4 + 4);
        try {
            buffer.writeInt(headerInBytes.length);
            buffer.writeBytes(headerInBytes);
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_KEY);
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_VALUE);
            //
            ack.setBody(buffer.array());
        } finally {
            buffer.release();
        }
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
        ByteBuf buffer = allocator.directBuffer(4 + headerInBytes.length + 4 + 4);
        try {
            buffer.writeInt(headerInBytes.length);
            buffer.writeBytes(headerInBytes);
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_KEY);
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_VALUE);
            //
            ack.setBody(buffer.array());
        } finally {
            buffer.release();
        }

        return ack;
    }

    public static Packet viewResp(long opaque, long msgId, Record<byte[], byte[]> record){
        Packet viewResp = new Packet();
        viewResp.setCmd(Command.VIEW_RESP.getCmd());
        viewResp.setOpaque(opaque);
        //
        Header header = new Header(record.getTopic(), record.getPartition(), record.getOffset(), msgId);
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);

        ByteBuf buffer = allocator.directBuffer(4 + headerInBytes.length + 4 + record.getKey().length + 4 + record.getValue().length);
        try {
            buffer.writeInt(headerInBytes.length);
            buffer.writeBytes(headerInBytes);
            buffer.writeInt(record.getKey().length);
            buffer.writeBytes(record.getKey());
            buffer.writeInt(record.getValue().length);
            buffer.writeBytes(record.getValue());
            //
            viewResp.setBody(buffer.array());
        } finally {
            buffer.release();
        }
        return viewResp;
    }

    public static Packet noViewMsgResp(long opaque){
        Packet viewResp = new Packet();
        viewResp.setCmd(Command.VIEW_RESP.getCmd());
        viewResp.setOpaque(opaque);
        viewResp.setBody(EMPTY_BODY);

        return viewResp;
    }

    public static Packet sendBackReq(Message message){
        Packet back = new Packet();
        back.setCmd(Command.SEND_BACK.getCmd());
        back.setOpaque(IdService.I.getId());
        //
        ByteBuf buffer = allocator.directBuffer(message.getHeaderInBytes().length + 4 + 4 + 4);
        try {
            buffer.writeInt(message.getHeaderInBytes().length);
            buffer.writeBytes(message.getHeaderInBytes());
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_KEY);
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_VALUE);
            //
            back.setBody(buffer.array());
        } finally {
            buffer.release();
        }
        return back;
    }

    public static Packet pullReq(long opaque){
        Packet pull = new Packet();
        pull.setCmd(Command.PULL_REQ.getCmd());
        pull.setOpaque(opaque);
        pull.setBody(EMPTY_BODY);

        return pull;
    }

    public static Packet pullNoMsgResp(long opaque){
        Packet result = new Packet();
        result.setOpaque(opaque);
        result.setCmd(Command.PULL_RESP.getCmd());
        //
        Header header = new Header(PullStatus.NO_NEW_MSG.getStatus());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        ByteBuf buffer = allocator.directBuffer(headerInBytes.length + 4 + 4 + 4);
        try {
            buffer.writeInt(headerInBytes.length);
            buffer.writeBytes(headerInBytes);
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_KEY);
            buffer.writeInt(0);
            buffer.writeBytes(EMPTY_VALUE);
            //
            result.setBody(buffer.array());
        } finally {
            buffer.release();
        }
        return result;
    }

}
