package com.tt.kafka.client.transport.codec;

import com.tt.kafka.client.transport.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

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
        out.writeLong(msg.getMsgId());
        out.writeInt(msg.getHeader().length);
        out.writeBytes(msg.getHeader());
        out.writeInt(msg.getKey().length);
        out.writeBytes(msg.getKey());
        out.writeInt(msg.getValue().length);
        out.writeBytes(msg.getValue());
    }

}

