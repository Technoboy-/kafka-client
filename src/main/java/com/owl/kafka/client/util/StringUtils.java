package com.owl.kafka.client.util;

/**
 * @Author: Tboy
 */
public class StringUtils {

    public static boolean isBlank(String str){
        return str == null || str.isEmpty();
    }

    public static byte[] getBytes(String str){
        return str.getBytes(Constants.UTF8);
    }

    public static String getString(byte[] bytes){
        if(bytes == null){
            return null;
        }
        return new String(bytes, Constants.UTF8);
    }
}
