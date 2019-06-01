package cn.westlife.redis.queue.annotation;

/**
 * @author westlife
 * @date 2019/2/27 21:45
 */
public enum RejectStrategy {
    /**
     * 丢弃
     */
    DISCARD,
    /**
     * 队尾
     */
    TAIL_QUEUE,
    /**
     * 队首
     */
    HEAD_QUEUE;

}
