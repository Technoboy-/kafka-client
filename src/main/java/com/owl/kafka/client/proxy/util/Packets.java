package com.owl.kafka.client.proxy.util;

import com.owl.kafka.client.consumer.Record;
import com.owl.kafka.client.proxy.transport.alloc.ByteBufferPool;
import com.owl.kafka.client.proxy.service.IdService;
import com.owl.kafka.client.proxy.service.PullStatus;
import com.owl.kafka.client.proxy.service.TopicPartitionOffset;
import com.owl.kafka.client.proxy.transport.message.Header;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.transport.protocol.Command;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.serializer.SerializerImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

/**
 * @Author: Tboy
 */
public class Packets {

    private static ByteBufferPool bufferPool = ByteBufferPool.DEFAULT;

    private static ByteBuffer EMPTY_BODY = ByteBuffer.allocate(0);

    private static ByteBuffer EMPTY_KEY = ByteBuffer.allocate(0);

    private static ByteBuffer EMPTY_VALUE = ByteBuffer.allocate(0);

    private static final ByteBuf PING_BUF;

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
        Packet packet = new Packet();
        packet.setCmd(Command.ACK.getCmd());
        packet.setOpaque(IdService.I.getId());

        Header header = new Header(msgId);
        header.setSign(Header.Sign.PUSH.getSign());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        //
        ByteBuffer buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + 4);
        buffer.putInt(headerInBytes.length);
        buffer.put(headerInBytes);
        buffer.putInt(0);
        buffer.put(EMPTY_KEY);
        buffer.putInt(0);
        buffer.put(EMPTY_VALUE);
        //
        packet.setBody(buffer);

        return packet;
    }

    public static Packet ackPullReq(TopicPartitionOffset topicPartitionOffset){
        Packet packet = new Packet();
        packet.setCmd(Command.ACK.getCmd());
        packet.setOpaque(IdService.I.getId());

        Header header = new Header(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition(), topicPartitionOffset.getOffset(), topicPartitionOffset.getMsgId());
        header.setSign(Header.Sign.PULL.getSign());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        //
        ByteBuffer buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + 4);
        buffer.putInt(headerInBytes.length);
        buffer.put(headerInBytes);
        buffer.putInt(0);
        buffer.put(EMPTY_KEY);
        buffer.putInt(0);
        buffer.put(EMPTY_VALUE);
        //
        packet.setBody(buffer);

        return packet;
    }

    public static Packet viewReq(long msgId){
        Packet packet = new Packet();
        packet.setCmd(Command.VIEW_REQ.getCmd());
        packet.setOpaque(IdService.I.getId());
        //
        Header header = new Header(msgId);
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        //
        ByteBuffer buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + 4);
        buffer.putInt(headerInBytes.length);
        buffer.put(headerInBytes);
        buffer.putInt(0);
        buffer.put(EMPTY_KEY);
        buffer.putInt(0);
        buffer.put(EMPTY_VALUE);
        //
        packet.setBody(buffer);

        return packet;
    }

    public static Packet viewResp(long opaque, long msgId, Record<byte[], byte[]> record){
        Packet viewResp = new Packet();
        viewResp.setCmd(Command.VIEW_RESP.getCmd());
        viewResp.setOpaque(opaque);
        //
        Header header = new Header(record.getTopic(), record.getPartition(), record.getOffset(), msgId);
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);

        ByteBuffer buffer = bufferPool.allocate(4 + headerInBytes.length + 4 + record.getKey().length + 4 + record.getValue().length);
        buffer.putInt(headerInBytes.length);
        buffer.put(headerInBytes);
        buffer.putInt(record.getKey().length);
        buffer.put(record.getKey());
        buffer.putInt(record.getValue().length);
        buffer.put(record.getValue());
        //
        viewResp.setBody(buffer);

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
        ByteBuffer buffer = bufferPool.allocate(message.getHeaderInBytes().length + 4 + 4 + 4);
        buffer.putInt(message.getHeaderInBytes().length);
        buffer.put(message.getHeaderInBytes());
        buffer.putInt(0);
        buffer.put(EMPTY_KEY);
        buffer.putInt(0);
        buffer.put(EMPTY_VALUE);
        //
        back.setBody(buffer);

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
        Packet packet = new Packet();
        packet.setOpaque(opaque);
        packet.setCmd(Command.PULL_RESP.getCmd());
        //
        Header header = new Header(PullStatus.NO_NEW_MSG.getStatus());
        byte[] headerInBytes = SerializerImpl.getFastJsonSerializer().serialize(header);
        ByteBuffer buffer = bufferPool.allocate(headerInBytes.length + 4 + 4 + 4);
        buffer.putInt(headerInBytes.length);
        buffer.put(headerInBytes);
        buffer.putInt(0);
        buffer.put(EMPTY_KEY);
        buffer.putInt(0);
        buffer.put(EMPTY_VALUE);
        //
        packet.setBody(buffer);

        return packet;
    }

}
