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
        if (in.readableBytes() < TO_HEADER_LENGTH) {
            return;
        }
        in.markReaderIndex();
        byte magic = in.readByte();
        if (magic != MAGIC) {
            ctx.close();
            return;
        }
        byte version = in.readByte();
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
        if (in.readableBytes() < KEY_SIZE) {
            in.resetReaderIndex();
            return;
        }
        int keyLength = in.readInt();
        if (in.readableBytes() < keyLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] key = new byte[keyLength];
        in.readBytes(key);

        //value
        if (in.readableBytes() < VALUE_SIZE) {
            in.resetReaderIndex();
            return;
        }
        int valueLength = in.readInt();
        if (in.readableBytes() < valueLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] value = new byte[valueLength];
        in.readBytes(value);
        //
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