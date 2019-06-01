package cn.westlife.redis.queue.handler;



import cn.westlife.redis.queue.event.SourceEvent;

import java.util.List;

/**
 * @author westlife
 * @date 2019/1/16 11:13
 */
public interface SourceEventHandler {

    boolean handle(SourceEvent sourceEvent);

    boolean handle(List<SourceEvent> sourceEvent);

    boolean failHandle(Throwable e);
}
