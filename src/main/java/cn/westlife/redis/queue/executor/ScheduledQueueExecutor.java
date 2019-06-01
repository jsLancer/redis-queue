package cn.westlife.redis.queue.executor;

import cn.westlife.redis.queue.annotation.RejectStrategy;
import cn.westlife.redis.queue.event.SourceEvent;
import cn.westlife.redis.queue.handler.SourceEventHandler;
import cn.westlife.redis.queue.queue.SourceEventQueue;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author westlife
 * @date 2019/3/2 15:10
 */
@Slf4j
public class ScheduledQueueExecutor extends QueueExecutor implements Runnable {

    public ScheduledQueueExecutor(SourceEventQueue eventQueue, int group, int size, SourceEventHandler eventHandler, long delayTime, RejectStrategy rejectStrategy) {
        super(eventQueue, group, size, eventHandler, delayTime, rejectStrategy);
    }

    @Override
    public void execute() {
        run();
    }

    @Override
    public boolean scheduled() {
        return true;
    }

    @Override
    public void run() {
        while (true) {
            List<SourceEvent> eventList = eventQueue.get(group, 0, size);
            try {
                if (CollectionUtils.isEmpty(eventList)) {
                    Thread.sleep(delayTime);
                    return;
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            List<SourceEvent> successList = new ArrayList<>();
            Iterator<SourceEvent> iterator = eventList.iterator();
            while (iterator.hasNext()) {
                long start = System.currentTimeMillis();
                SourceEvent sourceEvent = iterator.next();
                long score = (long) sourceEvent.timestamp();

                if (start < score) {
                    break;
                }
                try {
                    // 捕获单个事件异常,让循环可以继续
                    if (eventHandler.handle(sourceEvent)) {
                        successList.add(sourceEvent);
                    } else {
                        log.error(this.getClass().getSimpleName() + " event handle failed, event = " + JSON.toJSONString(sourceEvent));
                    }
                } catch (Exception e) {
                    log.error(this.getClass().getSimpleName() + " event handle failed, event = " + JSON.toJSONString(sourceEvent));
                    eventHandler.failHandle(e);
                }
            }

            if (successList.size() > 0) {
                // 成功后移除队列中的数据
                try {
                    if (!eventQueue.remove(group, successList)) {
                        log.error(this.getClass().getSimpleName() + " event remove failed, events = " + JSON.toJSONString(successList));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(this.getClass().getSimpleName() + "移除队列异常!", e);
                }
            }
        }
    }
}
