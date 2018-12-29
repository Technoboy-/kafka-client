package com.tt.kafka.netty.codec;

import com.tt.kafka.netty.protocol.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import static com.tt.kafka.netty.protocol.Packet.*;

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
        short magic = in.readShort();
        if (magic != MAGIC) {
            ctx.close();
            return;
        }
        short version = in.readShort();
        if (version != VERSION) {
            ctx.close();
            return;
        }
        byte cmd = in.readByte();
        long msgId = in.readLong();
        //header
        int headerLength = in.readInt();
        if (in.readableBytes() < headerLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] header = new byte[headerLength];
        in.readBytes(header);
        //key
        int keyLength = in.readInt();
        if (in.readableBytes() < keyLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] key = new byte[keyLength];
        in.readBytes(key);
        //value
        int valueLength = in.readInt();
        if (in.readableBytes() < valueLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] value = new byte[valueLength];
        in.readBytes(value);
        Packet p = new Packet();
        p.setCmd(cmd);
        p.setVersion(version);
        p.setMsgId(msgId);
        p.setHeader(header);
        p.setKey(key);
        p.setValue(value);
        out.add(p);
    }
}
