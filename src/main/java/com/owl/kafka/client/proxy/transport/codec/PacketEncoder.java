package com.owl.kafka.client.proxy.transport.codec;

import com.owl.kafka.client.proxy.transport.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;

/**
 * @Author: Tboy
 */
@Sharable
public class PacketEncoder extends MessageToByteEncoder<Packet> {

    protected void encode(ChannelHandlerContext ctx, Packet msg, ByteBuf out) throws Exception {
        if(msg == null){
            throw new Exception("encode msg is null");
        }
        out.writeByte(Packet.MAGIC);
        out.writeByte(Packet.VERSION);
        out.writeByte(msg.getCmd());
        out.writeLong(msg.getOpaque());
        out.writeInt(msg.getBody().readableBytes());
        out.writeBytes(msg.getBody());
    }

}

