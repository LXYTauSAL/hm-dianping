package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogic(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R get(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallBack,Long time, TimeUnit unit){
        //从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //判断是否存在
        //存在直接返回
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json,type);
        }
        if(json != null){
            return null;
        }
        //尝试获取互斥锁
        R r = dbFallBack.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(keyPrefix + id,"",2L,unit);
            return null;
        }
        //数据库 存在 写入redis
        this.set(keyPrefix+id,r,time,unit);
        //返回数据
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R,ID> R getWithLogic(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallBack,Long time,TimeUnit unit) {
        String key = keyPrefix + id;
        //从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isNotBlank(json)){
            //反序列化为对象后  命中判断过期时间
            RedisData redisData = JSONUtil.toBean(json, RedisData.class);
            JSONObject data = (JSONObject) redisData.getData();
            R r = JSONUtil.toBean(data, type);
            LocalDateTime expireTime = redisData.getExpireTime();
            if(expireTime.isAfter(LocalDateTime.now())){
                //未过期，返回店铺信息
                return r;
            }
            //过期
            //尝试获取互斥锁 判断是否获取互斥锁
            String lockKey =LOCK_SHOP_KEY+id;
            boolean isLock = tryLock(lockKey);
            if(isLock){
                //成功 开启独立线程 实现缓存重建
                CACHE_REBUILD_EXECUTOR.submit(()->{
                    try {
                        //重建缓存
                        R r1 = dbFallBack.apply(id);
                        this.setWithLogic(key,r1,time,unit);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        unlock(lockKey);
                    }
                });
            }
        }

        R r = dbFallBack.apply(id);
        this.setWithLogic(key,r,time,unit);
        //失败返回过期的店铺信息
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }


}
