package com.tt.kafka.client.transport.protocol;

import java.io.Serializable;

/**
 *
 *  * **************************************************************************************************
 *                                          Protocol
 *  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ - - - - - - - - - - - - - - - - - - -┐
 *       1   │    1    │    1    │     8     │       4       |                |     4      |             |      4       |
 *  ├ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ - - - - - - - - - ─ ─ ─ - ┤- - - - - - -- -|
 *           │         │         │           │               |                |            |             |              |                |
 *  │  Magic   Version     Cmd       MsgId      header size  |  header value  |  key size  |  key value  |  value size  |  value content |
 *           │         │         │           │               |                |            |             |              |                |
 *  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ — — - - - - - - - - - - - - - - - - -
 * @Author: Tboy
 */
public class Packet implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final byte MAGIC = (byte) 0xbabe;

    public static final byte VERSION = (byte)0x00;

    public static final int LENGTH = 1 + 1 + 1 + 8 + 4 + 4 + 4;

    private byte version;

    private byte cmd;

    private long msgId;

    private byte[] header;

    private byte[] key;

    private byte[] value;

    public short getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public byte[] getHeader() {
        return header;
    }

    public void setHeader(byte[] header) {
        this.header = header;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte getCmd() {
        return cmd;
    }

    public void setCmd(byte cmd) {
        this.cmd = cmd;
    }

    /**
     * @return the msgId
     */
    public long getMsgId() {
        return msgId;
    }

    /**
     * @param msgId
     *            the msgId to set
     */
    public void setMsgId(long msgId) {
        this.msgId = msgId;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (msgId ^ (msgId >>> 32));
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
        if (msgId != other.msgId)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Packet [cmd=" + cmd + ", msgId=" + msgId + ", bodyLen=" + (header.length + key.length + value.length) + ", version=" + version + "]";
    }

}
