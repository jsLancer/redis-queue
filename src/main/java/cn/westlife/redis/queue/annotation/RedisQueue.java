package cn.westlife.redis.queue.annotation;

import cn.westlife.redis.queue.QueueType;
import cn.westlife.redis.queue.handler.SourceEventHandler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author westlife
 * @date 2019/1/16 11:18
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RedisQueue {

    String key();

    QueueType type() default QueueType.ORDER;

    Class<? extends SourceEventHandler> handler();

    int size() default 10;

    int group() default 0;

    boolean scheduled() default false;

    long delayTime() default 1000;

    int expireTime() default 7 * 24 * 60 * 60;

    RejectStrategy rejectStrategy() default RejectStrategy.DISCARD;
}
