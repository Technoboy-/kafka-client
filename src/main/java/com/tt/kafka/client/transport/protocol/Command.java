package com.tt.kafka.client.transport.protocol;

/**
 * @Author: Tboy
 */
public enum Command {

    HEARTBEAT(1),

    PUSH(2),

    ACK(3),

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
