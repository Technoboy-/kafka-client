package com.owl.kafka.client.transport.codec;

import com.owl.kafka.client.transport.protocol.Message;
import com.owl.kafka.client.transport.protocol.Packet;
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
        out.writeLong(msg.getOpaque());
        for(Message message : msg.getMessageList()){
            out.writeInt(message.getHeader().length);
            out.writeBytes(message.getHeader());
            out.writeInt(message.getKey().length);
            out.writeBytes(message.getKey());
            out.writeInt(message.getValue().length);
            out.writeBytes(message.getValue());

        }
    }

}

