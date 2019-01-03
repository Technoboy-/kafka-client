package com.tt.kafka.client.netty.protocol;

/**
 * @Author: Tboy
 */
public enum Command {

    LOGIN(1),

    HEARTBEAT(2),

    HEARTBEAT_ACK(3),

    PUSH(4),

    ACK(5),

    UNKNOWN(-1);

    Command(int cmd) {
        this.cmd = (byte) cmd;
    }

    public final byte cmd;

    public static Command toCMD(byte b) {
        Command[] values = values();
        if (b > 0 && b < values.length)
            return values[b - 1];
        return UNKNOWN;
    }

    public byte getCmd() {
        return cmd;
    }
}
