package cn.westlife.redis.queue;


import cn.westlife.redis.queue.annotation.RedisQueue;
import cn.westlife.redis.queue.annotation.RejectStrategy;
import cn.westlife.redis.queue.event.SourceEvent;
import cn.westlife.redis.queue.executor.QueueExecutor;
import cn.westlife.redis.queue.executor.ScheduledQueueExecutor;
import cn.westlife.redis.queue.handler.SourceEventHandler;
import cn.westlife.redis.queue.queue.ListSourceEventQueue;
import cn.westlife.redis.queue.queue.SourceEventQueue;
import cn.westlife.redis.queue.queue.ZSetSourceEventQueue;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author westlife
 * @date 2019/1/16 11:12
 */
public class QueueScheduler {

    private Map<Class<?>, SourceEventQueue> queueMap;

    private Map<Class<?>, QueueExecutor> executorMap;


    private StringRedisTemplate redisTemplate;

    private ApplicationContext applicationContext;

    public QueueScheduler(StringRedisTemplate redisTemplate) {
        queueMap = new ConcurrentHashMap<>();
        executorMap = new ConcurrentHashMap<>();
        this.redisTemplate = redisTemplate;
    }

    public QueueScheduler register(Class<? extends SourceEvent>... events) {
        for (Class<? extends SourceEvent> event : events) {
            registerQueue(event);
        }
        return this;
    }

    public void queueInfo() {
    }

    public Map<Class<?>, SourceEventQueue> getQueueMap() {
        return this.queueMap;
    }


    private void registerQueue(Class<? extends SourceEvent> eventClass) {
        if (queueMap.get(eventClass) != null) {
            return;
        }
        RedisQueue redisQueue = eventClass.getAnnotation(RedisQueue.class);
        if (redisQueue == null) {
            throw new RuntimeException("not RedisQueue annotation");
        }

        String key = redisQueue.key();
        int group = redisQueue.group();
        Class<? extends SourceEventHandler> handlerCls = redisQueue.handler();
        QueueType type = redisQueue.type();
        long delayTime = redisQueue.delayTime();
        int size = redisQueue.size();
        int expireTime = redisQueue.expireTime();
        RejectStrategy strategy = redisQueue.rejectStrategy();
        boolean scheduled = redisQueue.scheduled();

        SourceEventHandler handler = applicationContext.getBean(handlerCls);

        SourceEventQueue queue = null;

        switch (type) {
            case ORDER:
                queue = new ListSourceEventQueue(redisTemplate.opsForList(), key, group, eventClass, expireTime);
                break;
            case PRIORITY:
                queue = new ZSetSourceEventQueue(redisTemplate.opsForZSet(), key, group, eventClass, expireTime);
                break;
            default:
                break;
        }

        if (queue == null) {
            throw new RuntimeException("not support redis type " + type);
        }
        queueMap.put(eventClass, queue);


        if (group > 0) {
            for (int i = 0; i < group; i++) {
                QueueExecutor executor = scheduled ? new ScheduledQueueExecutor(queue, i, size, handler, delayTime, strategy)
                        : new QueueExecutor(queue, i, size, handler, delayTime, strategy);
                executorMap.put(eventClass, executor);
            }
        } else {
            QueueExecutor executor = scheduled ? new ScheduledQueueExecutor(queue, group, size, handler, delayTime, strategy)
                    : new QueueExecutor(queue, group, size, handler, delayTime, strategy);
            executorMap.put(eventClass, executor);
        }
    }


    public void execute() {
        for (QueueExecutor executor : executorMap.values()) {
            if (executor.scheduled()) {
                continue;
            }
            executor.execute();
        }
    }

    public void schedule(Class<?> eventClass) {
        QueueExecutor executor = executorMap.get(eventClass);
        if (executor == null) {
            throw new RuntimeException("not register " + eventClass.getSimpleName() + " event");
        }
        if (!executor.scheduled()) {
            throw new RuntimeException(eventClass.getSimpleName() + " not support scheduled");
        }
        executor.execute();
    }

}
