package com.tt.kafka.client.util;

import com.tt.kafka.util.StringUtils;

/**
 * @Author: Tboy
 */
public class SystemPropertiesUtils {

    public static int getInt(String key, int def){
        String value = get(key);
        if(StringUtils.isBlank(value)){
            return def;
        }
        return Integer.valueOf(value);
    }

    public static String get(String key, String def){
        String value = get(key);
        if(StringUtils.isBlank(value)){
            return def;
        }
        return value;
    }

    public static String get(String key){
        return System.getProperty(key);
    }
}
