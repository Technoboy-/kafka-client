package com.owl.kafka.client.spi;

import java.lang.annotation.*;

/**
 * @Author: Tboy
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface SPI {

    /**
     * 缺省扩展点名。
     */
	String value() default "";

}