package com.owl.kafka.client.spi;

import java.lang.annotation.*;

/**
 * @Author: Tboy
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface MonitorConfig {

    boolean enable() default false;

    String name() default "";

    String value() default "console";


}
