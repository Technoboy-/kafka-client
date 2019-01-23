package com.owl.kafka.client.proxy.transport.protocol;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class PacketHeader implements Serializable {

    private byte cmd;

    private long opaque;

    private int bodyLength;

    public int getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }

    public byte getCmd() {
        return cmd;
    }

    public void setCmd(byte cmd) {
        this.cmd = cmd;
    }

    public long getOpaque() {
        return opaque;
    }

    public void setOpaque(long opaque) {
        this.opaque = opaque;
    }
}
