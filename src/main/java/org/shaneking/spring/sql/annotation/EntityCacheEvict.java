package org.shaneking.spring.sql.annotation;

import org.shaneking.skava.lang.String0;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface EntityCacheEvict {
  int clsIdx() default 0;

  int pKeyIdx() default -1;//begin 0, for parameter

  String pKeyPath() default String0.EMPTY;//a.b.c, for parameter
}
