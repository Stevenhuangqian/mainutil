package com.efun.mainland.util.redis.v2;

/**
 * redis集群下常用的功能操作方法
 *
 * @author Administrator
 */
public class RedisTemplate {

    protected RedisService redisService;

    public RedisService getRedisService() {
        return redisService;
    }

    public void setRedisService(RedisService redisService) {
        this.redisService = redisService;
    }

}
