package cn.westlife.redis.queue.event;

import java.io.Serializable;

/**
 * @author westlife
 * @date 2019/1/16 11:14
 */
public interface SourceEvent extends Serializable {
    long getId();
    long timestamp();
}
