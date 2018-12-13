package com.tt.kafka.util;

/**
 * @Author: Tboy
 */
public final class Preconditions {

    private Preconditions() {}

    public static void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }
}
