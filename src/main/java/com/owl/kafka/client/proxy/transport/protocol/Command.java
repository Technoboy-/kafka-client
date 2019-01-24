package com.owl.kafka.client.proxy.transport.protocol;

/**
 * @Author: Tboy
 */
public enum Command {

    PING(1),

    PONG(2),

    PUSH(3),

    ACK(4),

    UNREGISTER(5),

    VIEW_REQ(6),

    VIEW_RESP(7),

    PULL_REQ(8),

    PULL_RESP(9),

    ACK_PULL(10),

    SEND_BACK(11),

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
