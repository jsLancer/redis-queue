package cn.westlife.redis.queue;



import cn.westlife.redis.queue.event.SourceEvent;
import cn.westlife.redis.queue.queue.SourceEventQueue;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * @author westlife
 * @date 2019/2/25 22:00
 */
public class QueueTemplate {

    private Map<Class<?>, SourceEventQueue> queueMap;

    public QueueTemplate(Map<Class<?>, SourceEventQueue> queueMap) {
        this.queueMap = queueMap;
    }


    public void send(SourceEvent event) {
        Class<? extends SourceEvent> eventClass = event.getClass();
        SourceEventQueue eventQueue = queueMap.get(eventClass);
        if (eventQueue == null) {
            throw new RuntimeException("not support " + eventClass.getName());
        }
        eventQueue.add(event);
    }

    public void send(List<SourceEvent> events) {
        if (CollectionUtils.isEmpty(events)) {
            return;
        }
        Class<? extends SourceEvent> eventClass = events.get(0).getClass();
        SourceEventQueue eventQueue = queueMap.get(eventClass);
        if (eventQueue == null) {
            throw new RuntimeException("not support " + eventClass.getName());
        }
        eventQueue.batchAdd(events);
    }


}
