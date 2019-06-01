package cn.westlife.redis.queue.queue;



import cn.westlife.redis.queue.event.SourceEvent;

import java.util.List;

/**
 * @author westlife
 * @date 2019/1/16 11:22
 */
public interface SourceEventQueue<T extends SourceEvent> {

    String GROUP_SUFFIX = "%s.group.%s";

    boolean add(T t);

    boolean batchAdd(List<T> list);

    List<T> get(int group, int start, int end);

    long count(int group);

    boolean remove(int group, List<T> list);

    boolean remove(int group, T t);

    int getGroup();

    String getKey(int group);
}
