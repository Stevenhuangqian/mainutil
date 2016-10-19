package com.efun.mainland.util.db.annotation;

import java.lang.annotation.*;

/**
 * Created by Efun on 2016-08-24.<br/>
 * column(列)
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {ElementType.FIELD})
public @interface Column {
    String alias() default "";

    boolean unique() default false;

    boolean primary() default false;
}
