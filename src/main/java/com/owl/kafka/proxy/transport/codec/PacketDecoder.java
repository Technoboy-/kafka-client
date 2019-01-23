package com.owl.kafka.proxy.transport.codec;

import com.owl.kafka.proxy.transport.protocol.Packet;
import com.owl.kafka.proxy.transport.protocol.PacketHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

/**
 * @Author: Tboy
 */
public class PacketDecoder extends ReplayingDecoder<PacketDecoder.State> {

    public PacketDecoder(){
        super(State.MAGIC);
    }

    private final PacketHeader packetHeader = new PacketHeader();

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()){
            case MAGIC:
                checkMagic(in.readByte());
                checkpoint(State.VERSION);
            case VERSION:
                checkVersion(in.readByte());
                checkpoint(State.COMMAND);
            case COMMAND:
                packetHeader.setCmd(in.readByte());
                checkpoint(State.OPAQUE);
            case OPAQUE:
                packetHeader.setOpaque(in.readLong());
                checkpoint(State.BODY_LENGTH);
            case BODY_LENGTH:
                packetHeader.setBodyLength(in.readInt());
                checkpoint(State.BODY);
            case BODY:
                byte[] body = new byte[packetHeader.getBodyLength()];
                in.readBytes(body);
                //
                Packet packet = new Packet();
                packet.setCmd(packetHeader.getCmd());
                packet.setOpaque(packetHeader.getOpaque());
                packet.setBody(body);
                out.add(packet);
                //
                checkpoint(State.MAGIC);
        }
    }

    private void checkMagic(byte magic) {
        if (magic != Packet.MAGIC) {
            throw new IllegalArgumentException("illegal packet [magic]" + magic);
        }
    }

    private void checkVersion(byte version) {
        if (version != Packet.VERSION) {
            throw new IllegalArgumentException("illegal packet [version]" + version);
        }
    }

    enum State{
        MAGIC,
        VERSION,
        COMMAND,
        OPAQUE,
        BODY_LENGTH,
        BODY;
    }
}
