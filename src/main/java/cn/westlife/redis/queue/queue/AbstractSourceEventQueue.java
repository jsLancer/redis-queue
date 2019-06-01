package cn.westlife.redis.queue.queue;


import cn.westlife.redis.queue.event.SourceEvent;

/**
 * @author westlife
 * @date 2019/1/16 11:21
 */
public abstract class AbstractSourceEventQueue implements SourceEventQueue {

    private String key;

    private int group;

    protected int expireTime;

    protected final Class<? extends SourceEvent> eventClass;

//    protected StringRedisTemplate redisTemplate;

    public AbstractSourceEventQueue(String key, int group, Class<? extends SourceEvent> eventClass, int expireTime) {
//        this.redisTemplate = redisTemplate;
        this.key = key;
        this.group = group;
        this.expireTime = expireTime;
        this.eventClass = eventClass;
    }


    private int getGroup(long id) {
        return (int) (id % group);
    }

    protected String getKey(long id) {
        if (group > 0) {
            return String.format(GROUP_SUFFIX, key, getGroup(id));
        }
        return key;
    }

    @Override
    public String getKey(int group) {
        if (group > 0) {
            return String.format(GROUP_SUFFIX, key, group);
        }
        return key;
    }


    @Override
    public int getGroup() {
        return group;
    }

}
