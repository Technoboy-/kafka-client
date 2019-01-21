package com.owl.kafka.client.transport.protocol;

import java.io.Serializable;

/**
 *
 *  * **************************************************************************************************
 *                                          Protocol
 *  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ - - - - - - - - - - - - - - - - - - -┐
 *       1   │    1    │    1    │     8     │       4       |                |     4      |             |      4       |
 *  ├ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ - - - - - - - - - ─ ─ ─ - ┤- - - - - - -- -|
 *           │         │         │           │               |                |            |             |              |                |
 *  │  Magic   Version     Cmd       Opaque      header size  |  header value  |  key size  |  key value  |  value size  |  value content |
 *           │         │         │           │               |                |            |             |              |                |
 *  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ — — - - - - - - - - - - - - - - - - -
 * @Author: Tboy
 */
public class Packet implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final byte MAGIC = (byte) 0xbabe;

    public static final byte VERSION = (byte)0x00;

    public static final int TO_HEADER_LENGTH = 1 + 1 + 1 + 8 + 4;

    public static final int KEY_SIZE = 4;

    public static final int VALUE_SIZE = 4;

    public Packet(){
        //NOP
    }

    public Packet(long opaque){
        this.opaque = opaque;
    }

    private byte version;

    private byte cmd;

    private long opaque;

    private byte[] header;

    private Header headerRef;

    private byte[] key;

    private byte[] value;

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public Header getHeaderRef() {
        return headerRef;
    }

    public void setHeaderRef(Header headerRef) {
        this.headerRef = headerRef;
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

    public long getOpaque() {
        return opaque;
    }

    public void setOpaque(long opaque) {
        this.opaque = opaque;
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
        return "Packet [cmd=" + cmd + ", opaque=" + opaque + ", bodyLen=" + (header.length + key.length + value.length) + ", version=" + version + "]";
    }

}
