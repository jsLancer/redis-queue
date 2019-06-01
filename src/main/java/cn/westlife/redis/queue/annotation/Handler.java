package cn.westlife.redis.queue.annotation;



import cn.westlife.redis.queue.handler.SourceEventHandler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author westlife
 * @date 2018/11/4 15:22
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Handler {

    Class<? extends SourceEventHandler> value();
}
