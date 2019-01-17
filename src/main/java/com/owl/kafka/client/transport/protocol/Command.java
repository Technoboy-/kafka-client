package com.owl.kafka.client.transport.protocol;

/**
 * @Author: Tboy
 */
public enum Command {

    PING(1),

    PONG(2),

    PUSH(3),

    ACK(4),

    UNREGISTER(5),

    VIEW(6),

    PULL(7),

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
