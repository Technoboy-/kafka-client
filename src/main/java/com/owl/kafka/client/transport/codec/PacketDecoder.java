package com.owl.kafka.client.transport.codec;

import com.owl.kafka.client.transport.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import static com.owl.kafka.client.transport.protocol.Packet.*;

/**
 * @Author: Tboy
 */
public class PacketDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < LENGTH) {
            return;
        }
        in.markReaderIndex();
        //
        byte magic = in.readByte();
        if (magic != MAGIC) {
            ctx.close();
            return;
        }
        //
        byte version = in.readByte();
        if (version != VERSION) {
            ctx.close();
            return;
        }
        //cmd
        byte cmd = in.readByte();
        //opaque
        long opaque = in.readLong();
        //body length
        int bodyLength = in.readInt();
        if (in.readableBytes() < bodyLength) {
            in.resetReaderIndex();
            return;
        }
        //
        byte[] body = new byte[bodyLength];
        in.readBytes(body);

        //
        Packet p = new Packet();
        p.setVersion(version);
        p.setCmd(cmd);
        p.setOpaque(opaque);
        p.setBody(body);
        out.add(p);
    }
}
