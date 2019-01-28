package com.owl.kafka.client.proxy.transport.protocol;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 *
 *  * *************************************************************************
 *                                   Protocol
 *  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *       1   │    1    │    1    │     8     │       4       |                |
 *  ├ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *           │         │         │           │               |                |
 *  │  magic   version     cmd       opaque      body size   |  body value    |
 *           │         │         │           │               |                |
 *  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 * @Author: Tboy
 */
public class Packet implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final byte MAGIC = (byte) 0xbabe;

    public static final byte VERSION = (byte)0x00;

    public static final int LENGTH = 1 + 1 + 1 + 8 + 4;

    public Packet(){
    }

    public Packet(long opaque){
        this.opaque = opaque;
    }

    private byte version;

    private byte cmd;

    private long opaque;

    private ByteBuffer body;

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
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

    public ByteBuffer getBody() {
        return body;
    }

    public void setBody(ByteBuffer body) {
        this.body = body;
    }

    public boolean isBodyEmtpy(){
        return this.body == null || this.body.limit() - this.body.position() == this.body.capacity();
    }

    public int getBodyLength(){
        return this.body.remaining();
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (opaque ^ (opaque >>> 32));
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Packet other = (Packet) obj;
        if (opaque != other.opaque)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Packet [cmd=" + cmd + ", opaque=" + opaque + ", bodyLen=" + (body == null ? 0 : body.capacity()) + ", version=" + version + "]";
    }

}
