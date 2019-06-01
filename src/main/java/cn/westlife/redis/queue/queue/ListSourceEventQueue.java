package cn.westlife.redis.queue.queue;


import cn.westlife.redis.queue.event.SourceEvent;
import com.alibaba.fastjson.JSON;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author westlife
 * @date 2019/1/16 11:26
 */
public class ListSourceEventQueue extends AbstractSourceEventQueue {


    private ListOperations<String, String> list;

    public ListSourceEventQueue(ListOperations<String, String> list, String key, int group, Class<? extends SourceEvent> eventClass, int expireTime) {
        super(key, group, eventClass, expireTime);
        this.list = list;
    }

    @Override
    public boolean add(SourceEvent event) {
        String key = getKey(event.getId());
        Long result = list.rightPush(key, JSON.toJSONString(event));
        list.getOperations().expire(key, expireTime, TimeUnit.SECONDS);
        return result != null && result > 0;
    }

    @Override
    public boolean batchAdd(List list) {
        if (CollectionUtils.isEmpty(list)) {
            return true;
        }
        List<Object> resp = this.list.getOperations().executePipelined(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.openPipeline();
                for (Object data : list) {
                    SourceEvent event = (SourceEvent) data;
                    String key = getKey(event.getId());
                    Long push = redisConnection.rPush(key.getBytes(), JSON.toJSONString(event).getBytes());
                    System.out.println(push);
                }
//                List<Object> resp = redisConnection.closePipeline();
                return true;
            }
        });
        boolean result = true;
        for (Object response : resp) {
            Long n = (Long) response;
            if (n == null || n <= 0) {
                result = false;
            }
        }
        return result;
    }

    @Override
    public List get(int group, int start, int end) {
        String key = getKey(group);
        List<String> result = list.range(key, start, end);
        if (CollectionUtils.isEmpty(result)) {
            return new ArrayList(0);
        }
        return result.stream().map(e -> JSON.parseObject(e, eventClass)).collect(Collectors.toList());
    }

    @Override
    public long count(int group) {
        String key = getKey(group);
        Long size = list.size(key);
        return size == null ? 0 : size;
    }

    @Override
    public boolean remove(int group, List list) {
        String key = getKey(group);

        List<Object> resp = this.list.getOperations().executePipelined(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.openPipeline();

                for (Object o : list) {
                    String s = JSON.toJSONString(o);
                    Long rem = redisConnection.lRem(key.getBytes(), 1, s.getBytes());
                    System.out.println(rem);
                }
                return true;
            }
        });
        boolean result = true;
        for (Object response : resp) {
            Long n = (Long) response;
            if (n == null || n <= 0) {
                result = false;
            }
        }
        return result;
    }

    @Override
    public boolean remove(int group, SourceEvent event) {
        String key = getKey(group);
        String s = JSON.toJSONString(event);
        Long resp = list.remove(key, 1, s);
        return resp != null && resp > 0;
    }


}
