package cn.westlife.redis.queue.executor;


import cn.westlife.redis.queue.annotation.RejectStrategy;
import cn.westlife.redis.queue.event.SourceEvent;
import cn.westlife.redis.queue.handler.SourceEventHandler;
import cn.westlife.redis.queue.queue.SourceEventQueue;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author westlife
 * @date 2019/2/28 17:37
 */
@Slf4j
public class QueueExecutor implements Runnable {

    protected SourceEventQueue eventQueue;

    private Thread thread;

    protected long delayTime;

    protected RejectStrategy rejectStrategy;

    protected SourceEventHandler eventHandler;

    protected int group;

    protected int size;


    public QueueExecutor(SourceEventQueue eventQueue, int group, int size, SourceEventHandler eventHandler, long delayTime, RejectStrategy rejectStrategy) {
        this.eventQueue = eventQueue;
        this.delayTime = delayTime;
        this.rejectStrategy = rejectStrategy;
        this.eventHandler = eventHandler;
        this.group = group;
        this.size = size;
        if (!scheduled()) {
            this.thread = new Thread(this, eventQueue.getKey(group));
        }
    }

    public void execute() {
        thread.start();
    }

    public boolean scheduled() {
        return false;
    }

    @Override
    public void run() {
        while (true) {
            SourceEventHandler sourceEventHandler = eventHandler;
            List<SourceEvent> eventList = eventQueue.get(group, 0, size);
            try {
                if (CollectionUtils.isEmpty(eventList)) {
                    if (delayTime > 0) {
                        Thread.sleep(delayTime);
                    }
                    return;
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }

            List<SourceEvent> successList = new ArrayList<>();
            try {
                for (SourceEvent sourceEvent : eventList) {
                    if (sourceEventHandler.handle(sourceEvent)) {
                        successList.add(sourceEvent);
                    } else {
                        log.error(sourceEventHandler.getClass().getSimpleName() + " event handle failed, event = " + JSON.toJSONString(sourceEvent));
                    }
                }
            } catch (Exception e) {
                sourceEventHandler.failHandle(e);
            }
            if (successList.size() > 0) {
                // 成功后移除队列中的数据
                try {
                    if (!eventQueue.remove(group, successList)) {
                        log.error(sourceEventHandler.getClass().getSimpleName() + " event remove failed, events = " + JSON.toJSONString(successList));
                    }
                } catch (Exception e) {
                    sourceEventHandler.failHandle(e);
                    throw new RuntimeException(sourceEventHandler.getClass().getSimpleName() + "移除队列异常!", e);
                }
            }
        }
    }
}
