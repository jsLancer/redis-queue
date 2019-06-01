package cn.westlife.redis.queue.queue;


import cn.westlife.redis.queue.event.SourceEvent;
import com.alibaba.fastjson.JSON;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author westlife
 * @date 2019/1/16 11:27
 */
public class ZSetSourceEventQueue extends AbstractSourceEventQueue {

    private ZSetOperations<String, String> zset;

    public ZSetSourceEventQueue(ZSetOperations<String, String> zset, String key, int group, Class<? extends SourceEvent> eventClass, int expireTime) {
        super(key, group, eventClass, expireTime);
        this.zset = zset;
    }

    @Override
    public boolean add(SourceEvent event) {
        String key = getKey(event.getId());
        Boolean result = zset.add(key, JSON.toJSONString(event), event.timestamp());
        if (result) {
            zset.getOperations().expire(key, expireTime, TimeUnit.SECONDS);
        }
        return result;
    }

    @Override
    public boolean batchAdd(List list) {
        if (CollectionUtils.isEmpty(list)) {
            return true;
        }

        List<Object> resp = zset.getOperations().executePipelined(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection redisConnection) throws DataAccessException {
                redisConnection.openPipeline();
                for (Object data : list) {
                    SourceEvent event = (SourceEvent) data;
                    String key = getKey(event.getId());
                    Boolean add = redisConnection.zAdd(key.getBytes(), event.timestamp(), JSON.toJSONString(event).getBytes());
                    System.out.println(add);
                }
                return null;
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
        Set<String> set = zset.range(key, start, end);
        return set.stream().map(e -> JSON.parseObject(e, eventClass)).collect(Collectors.toList());
    }

    @Override
    public long count(int group) {
        String key = getKey(group);
        Long result = zset.zCard(key);
        return result == null ? 0 : result;
    }

    @Override
    public boolean remove(int group, List list) {
        String key = getKey(group);
        boolean result = true;
        if (list != null && !list.isEmpty()) {
            String[] values = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                values[i] = JSON.toJSONString(list.get(i));
            }
            Long removeResult = zset.remove(key, values);
            result = removeResult != null && removeResult == values.length;
        }
        return result;
    }

    @Override
    public boolean remove(int group, SourceEvent event) {
        String key = getKey(group);
        Long result = zset.remove(key, JSON.toJSONString(event));
        return result != null && result > 0;
    }
}
