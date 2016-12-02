package com.efun.mainland.util.redis.v2;

import com.efun.mainland.util.CommonUtil;
import com.efun.mainland.util.cache.TimeUnitSeconds;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.params.geo.GeoRadiusParam;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by Efun on 2016-10-31.
 */
public class ClientCluster extends ShardedJedisSentinelPool implements RedisService {
    private static final Logger log = LoggerFactory.getLogger(ClientCluster.class);
    private final Timer timer = new Timer("ClientCluster-delay-clear-delete-cache-timer");
    private long intervalTime = 5 * 60 * 1000;
    private String cachePrefix = "";
    /**
     * 为了避免数据库主从同步延迟导致的问题，需要再次删除缓存key值的队列key
     */
    private String CACHE_KEYS_QUEUE_STRING;
    private byte[] CACHE_KEYS_QUEUE_BYTE;

    public ClientCluster(Set<String> masterNames, Set<String> sentinels,
                         final GenericObjectPoolConfig poolConfig) {
        this(masterNames, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
    }

    public ClientCluster(Set<String> masterNames, Set<String> sentinels) {
        this(masterNames, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, null,
                Protocol.DEFAULT_DATABASE);
    }

    public ClientCluster(Set<String> masterNames, Set<String> sentinels, String password) {
        this(masterNames, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, password);
    }

    public ClientCluster(Set<String> masterNames, Set<String> sentinels,
                         final GenericObjectPoolConfig poolConfig, int timeout, final String password) {
        this(masterNames, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
    }

    public ClientCluster(Set<String> masterNames, Set<String> sentinels,
                         final GenericObjectPoolConfig poolConfig, final int timeout) {
        this(masterNames, sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE);
    }

    public ClientCluster(Set<String> masterNames, Set<String> sentinels,
                         final GenericObjectPoolConfig poolConfig, final String password) {
        this(masterNames, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password);
    }

    /**
     * Master-Slave must be use the same config
     */
    public ClientCluster(Set<String> masterNames, Set<String> sentinels,
                         final GenericObjectPoolConfig poolConfig, int timeout, final String password, final int database) {
        super(masterNames, sentinels, poolConfig, timeout, StringUtils.isBlank(password) ? null : password, database);
        log.info("masterNames={},sentinels={}", masterNames, sentinels);
        init();
    }

    public ClientCluster(String masters, String sentinels,
                         final GenericObjectPoolConfig poolConfig, int timeout, final String password) {
        this(RedisHelperUtil.string2StringSet(masters), RedisHelperUtil.string2StringSet(sentinels), poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
    }

    public void setIntervalTime(long intervalTime) {
        if (intervalTime > 100)
            this.intervalTime = intervalTime;
    }

    public void setCachePrefix(String cachePrefix) {
        if (cachePrefix != null && cachePrefix.length() > 0)
            this.cachePrefix = cachePrefix;
    }

    private void init() {
        if (Charset.isSupported("UTF-8")) {
            System.out.println("UTF-8 Charset SupportedEncoding");
            log.info("UTF-8 Charset SupportedEncoding");
        } else {
            System.out.println("UTF-8 Charset UnsupportedEncoding");
            log.error("UTF-8 Charset UnsupportedEncoding");
        }
        CACHE_KEYS_QUEUE_STRING = loadCachePrefix() + "_CACHE_KEYS_QUEUE_STRING";
        try {
            CACHE_KEYS_QUEUE_BYTE = (loadCachePrefix() + "_CACHE_KEYS_QUEUE_BYTE").getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {            // TODO Auto-generated catch block
            log.error("UTF-8 UnsupportedEncoding", e);
        }
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                final int pageSize = 5000;
                try {
                    Set<String> tempSet;
                    do {
                        tempSet = zrangeByScore(CACHE_KEYS_QUEUE_STRING, 0,
                                System.currentTimeMillis() - intervalTime, 0, pageSize);
                        if (tempSet != null && tempSet.size() > 0) {
                            String[] temps = new String[tempSet.size()];
                            int i = 0;
                            for (String temp : tempSet) {
                                temps[i] = temp;
                                i++;
                                del(temp, false);
                                log.debug("delete redis string key:" + temp);
                            }
                            log.info("delete redis string key size:" + i);
                            zrem(CACHE_KEYS_QUEUE_STRING, temps);
                        }
                    } while (tempSet != null && tempSet.size() == pageSize);
                    Set<byte[]> tempSet1;
                    do {
                        tempSet1 = zrangeByScore(CACHE_KEYS_QUEUE_BYTE, 0,
                                System.currentTimeMillis() - intervalTime, 0, pageSize);
                        if (tempSet1 != null && tempSet1.size() > 0) {
                            byte[][] temps = new byte[tempSet1.size()][];
                            int i = 0;
                            for (byte[] temp : tempSet1) {
                                temps[i] = temp;
                                i++;
                                del(temp, false);
                            }
                            log.info("delete redis byte key size:" + i);
                            zrem(CACHE_KEYS_QUEUE_BYTE, temps);
                        }
                    } while (tempSet1 != null && tempSet1.size() == pageSize);
                } catch (Throwable e) {
                    log.error("Throwable:" + e.getMessage(), e);
                }
            }
        }, intervalTime, intervalTime);
    }

    public boolean isServerCluster() {
        return false;
    }

    public void releaseResource(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                log.error("Closeable Exception:" + e.getMessage(), e);
            }
        }
    }

    public Long append(byte[] key, byte[] value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.append(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long append(String key, String value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.append(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long bitcount(byte[] key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.bitcount(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long bitcount(byte[] key, long start, long end) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.bitcount(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long bitcount(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.bitcount(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long bitcount(String key, long start, long end) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.bitcount(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<String> blpop(int timeout, String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.blpop(timeout, key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<String> blpop(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.blpop(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<String> brpop(int timeout, String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.brpop(timeout, key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<String> brpop(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.brpop(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long decr(byte[] key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.decr(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long decr(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.decr(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long decrBy(byte[] key, long value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.decrBy(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long decrBy(String key, long value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.decrBy(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long del(byte[] key, boolean delay) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            if (delay) {                // 间隔一段时间后再次执行，避免数据库主从同步延迟导致的问题
                redis.zadd(CACHE_KEYS_QUEUE_BYTE, System.currentTimeMillis(), key);
            }
            return redis.del(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long del(String key, boolean delay) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            if (delay) {                // 间隔一段时间后再次执行，避免数据库主从同步延迟导致的问题
                redis.zadd(CACHE_KEYS_QUEUE_STRING, System.currentTimeMillis(), key);
            }
            return redis.del(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Boolean exists(byte[] key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.exists(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Boolean exists(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.exists(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long expire(byte[] key, int seconds) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.expire(key, seconds);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long expire(String key, int seconds) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.expire(key, seconds);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long expireAt(byte[] key, long unixTime) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.expireAt(key, unixTime);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long expireAt(String key, long unixTime) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.expireAt(key, unixTime);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public byte[] get(byte[] key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.get(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String get(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.get(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String getrange(String key, long startOffset, long endOffset) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.getrange(key, startOffset, endOffset);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public byte[] getSet(byte[] key, byte[] value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.getSet(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String getSet(String key, String value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.getSet(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hdel(byte[] key, byte[]... fields) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hdel(key, fields);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hdel(String key, String... fields) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hdel(key, fields);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Boolean hexists(byte[] key, byte[] field) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hexists(key, field);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Boolean hexists(String key, String field) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hexists(key, field);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public byte[] hget(byte[] key, byte[] field) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hget(key, field);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String hget(String key, String field) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hget(key, field);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hgetAll(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Map<String, String> hgetAll(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hgetAll(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hincrBy(byte[] key, byte[] field, long value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hincrBy(key, field, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hincrBy(String key, String field, long value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hincrBy(key, field, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hincrByFloat(key, field, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Double hincrByFloat(String key, String field, double value) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hincrByFloat(key, field, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> hkeys(byte[] key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hkeys(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> hkeys(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hkeys(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hlen(byte[] key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hlen(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hlen(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hlen(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hmget(key, fields);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<String> hmget(String key, String... fields) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hmget(key, fields);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.hmset(key, hash);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String hmset(String key, Map<String, String> hash) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.hmset(key, hash);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.hset(key, field, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hset(String key, String field, String value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.hset(key, field, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.hsetnx(key, field, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long hsetnx(String key, String field, String value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.hsetnx(key, field, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Collection<byte[]> hvals(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.hvals(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<String> hvals(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.hvals(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long incr(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.incr(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long incr(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.incr(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long incrBy(byte[] key, long value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.incrBy(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long incrBy(String key, long value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.incrBy(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    // /**
    // * ShardedJedis中不支持
    // */
    // public Long pexpire(byte[] key, long milliseconds) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.pexpire(key, milliseconds);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    // /**
    // * ShardedJedis中不支持
    // */
    // public Long pexpire(String key, long milliseconds) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.pexpire(key, milliseconds);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    // /**
    // * ShardedJedis中不支持
    // */
    // public Long pexpireAt(byte[] key, long milliseconds) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.pexpireAt(key, milliseconds);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    // /**
    // * ShardedJedis中不支持
    // */
    // public Long pexpireAt(String key, long milliseconds) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.pexpireAt(key, milliseconds);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    // /**
    // * ShardedJedis中不支持
    // */
    // public String rename(byte[] oldkey, byte[] newkey) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.rename(oldkey, newkey);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    // /**
    // * ShardedJedis中不支持
    // */
    // public String rename(String oldkey, String newkey) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.rename(oldkey, newkey);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    // /**
    // * ShardedJedis中不支持
    // */
    // public Long renamenx(byte[] oldkey, byte[] newkey) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.renamenx(oldkey, newkey);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    // /**
    // * ShardedJedis中不支持
    // */
    // public Long renamenx(String oldkey, String newkey) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.renamenx(oldkey, newkey);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    public Double incrByFloat(byte[] key, double value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.incrByFloat(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Double incrByFloat(String key, double value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.incrByFloat(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public byte[] lindex(byte[] key, long index) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lindex(key, index);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String lindex(String key, long index) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lindex(key, index);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long llen(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.llen(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long llen(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.llen(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public byte[] lpop(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lpop(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String lpop(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lpop(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long lpush(byte[] key, byte[]... strings) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lpush(key, strings);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long lpush(String key, String... strings) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lpush(key, strings);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long lpushx(byte[] key, byte[]... strings) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lpushx(key, strings);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long lpushx(String key, String... strings) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lpushx(key, strings);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<byte[]> lrange(byte[] key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lrange(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<String> lrange(String key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lrange(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long lrem(byte[] key, long count, byte[] value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lrem(key, count, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long lrem(String key, long count, String value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lrem(key, count, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String lset(byte[] key, long index, byte[] value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lset(key, index, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String lset(String key, long index, String value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.lset(key, index, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String ltrim(byte[] key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.ltrim(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String ltrim(String key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.ltrim(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long persist(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.persist(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long persist(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.persist(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    // /**
    // * ShardedJedis中不支持
    // */
    // public Set<byte[]> sunion(byte[]... keys) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.sunion(keys);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    // /**
    // * ShardedJedis中不支持
    // */
    // public Set<String> sunion(String... keys) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.sunion(keys);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    public Long pfadd(byte[] key, byte[]... elements) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.pfadd(key, elements);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long pfadd(String key, String... elements) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.pfadd(key, elements);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public long pfcount(byte[] key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.pfcount(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return 0L;
    }

    public long pfcount(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.pfcount(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return 0L;
    }

    public byte[] rpop(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.rpop(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String rpop(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.rpop(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long rpush(byte[] key, byte[]... strings) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.rpush(key, strings);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long rpush(String key, String... strings) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.rpush(key, strings);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long rpushx(byte[] key, byte[]... strings) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.rpushx(key, strings);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long rpushx(String key, String... strings) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.rpushx(key, strings);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long sadd(byte[] key, byte[]... members) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.sadd(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long sadd(String key, String... members) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.sadd(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long scard(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.scard(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long scard(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.scard(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String set(byte[] key, byte[] value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.set(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String set(String key, String value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.set(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.set(key, value, nxxx, expx, time);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String set(String key, String value, String nxxx, String expx, long time) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.set(key, value, nxxx, expx, time);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String setex(byte[] key, int seconds, byte[] value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.setex(key, seconds, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String setex(String key, int seconds, String value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.setex(key, seconds, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long setnx(byte[] key, byte[] value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.setnx(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long setnx(String key, String value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.setnx(key, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long setrange(byte[] key, long offset, byte[] value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.setrange(key, offset, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long setrange(String key, long offset, String value) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.setrange(key, offset, value);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Boolean sismember(byte[] key, byte[] member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.sismember(key, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Boolean sismember(String key, String member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.sismember(key, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> smembers(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.smembers(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> smembers(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.smembers(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<byte[]> sort(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.sort(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.sort(key, sortingParameters);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<String> sort(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.sort(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public List<String> sort(String key, SortingParams sortingParameters) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.sort(key, sortingParameters);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    // /**
    // * ShardedJedis中不支持
    // */
    // public Long pttl(String key) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.pttl(key);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    // /**
    // * ShardedJedis中不支持
    // */
    // public Long pttl(byte[] key) { //
    // ShardedJedis redis = null; //
    //
    //
    // try { // redis = getResource();
    // return  redis.pttl(key);
    // } catch (Exception e) { //
    //
    // log.error("pool or redis object or command exception:"+e.getMessage(),e);
    // } finally { // Redis.returnRedis(redis,isBroken);
    // }
    // return null;
    // }
    public byte[] spop(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.spop(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String spop(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.spop(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long srem(byte[] key, byte[]... members) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.srem(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long srem(String key, String... members) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.srem(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long strlen(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.strlen(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long strlen(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.strlen(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long ttl(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.ttl(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long ttl(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.ttl(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String type(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.type(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public String type(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.type(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zadd(byte[] key, double score, byte[] member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zadd(key, score, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zadd(key, scoreMembers);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zadd(String key, double score, String member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zadd(key, score, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zadd(String key, Map<String, Double> scoreMembers) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zadd(key, scoreMembers);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zcard(byte[] key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zcard(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zcard(String key) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zcard(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zcount(byte[] key, byte[] min, byte[] max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zcount(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zcount(byte[] key, double min, double max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zcount(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zcount(String key, double min, double max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zcount(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zcount(String key, String min, String max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zcount(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Double zincrby(byte[] key, double score, byte[] member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zincrby(key, score, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Double zincrby(String key, double score, String member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zincrby(key, score, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zlexcount(byte[] key, byte[] min, byte[] max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zlexcount(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zlexcount(String key, String min, String max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zlexcount(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrange(byte[] key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrange(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrange(String key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrange(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByLex(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByLex(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrangeByLex(String key, String min, String max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByLex(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByLex(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScore(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScore(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrangeByScore(String key, double min, double max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScore(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrangeByScore(String key, String min, String max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScore(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScore(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScoreWithScores(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScoreWithScores(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScoreWithScores(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScoreWithScores(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeWithScores(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrangeWithScores(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zrank(byte[] key, byte[] member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrank(key, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zrank(String key, String member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrank(key, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zrem(byte[] key, byte[]... members) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrem(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zrem(String key, String... members) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrem(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zremrangeByLex(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zremrangeByLex(String key, String min, String max) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zremrangeByLex(key, min, max);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zremrangeByRank(byte[] key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zremrangeByRank(String key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zremrangeByScore(byte[] key, double start, double end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zremrangeByScore(String key, double start, double end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zremrangeByScore(String key, String start, String end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zremrangeByScore(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrange(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrevrange(String key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrange(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByLex(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByLex(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrevrangeByLex(String key, String max, String min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByLex(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByLex(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScore(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScore(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScore(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScore(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrevrangeByScore(String key, double max, double min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScore(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScore(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrevrangeByScore(String key, String max, String min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScore(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScore(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScoreWithScores(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScoreWithScores(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScoreWithScores(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScoreWithScores(key, max, min);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeWithScores(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrangeWithScores(key, start, end);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zrevrank(byte[] key, byte[] member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrank(key, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Long zrevrank(String key, String member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zrevrank(key, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Double zscore(byte[] key, byte[] member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zscore(key, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public Double zscore(String key, String member) {
        ShardedJedis redis = null;

        try {
            redis = getResource();
            return redis.zscore(key, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    public boolean tryLock(String key, boolean permanent) {
        ShardedJedis redis = null;
        while (true) {
            try {
                redis = getResource();
                if (permanent) {
                    Long temp = redis.setnx(key, "1");
                    if (temp != null && temp.intValue() > 0) {
                        return true;
                    }
                } else {
                    String temp = redis.set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX, TimeUnitSeconds.CACHE_1_HOUR);
                    if ("OK".equals(temp)) {
                        return true;
                    }
                }
            } catch (Exception e) {
                log.error("pool or redis object or command exception:" + e.getMessage(), e);
            } finally {
                releaseResource(redis);
            }
        }
    }

    public boolean tryLock(String key, long timeoutMillis) {
        if (timeoutMillis <= 0L) {
            return tryLock(key, false);
        } else {
            ShardedJedis redis = null;
            long startTime = System.currentTimeMillis();
            long currentTime = startTime;
            while (currentTime - startTime < timeoutMillis) {
                try {
                    redis = getResource();
                    String temp = redis.set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX, TimeUnitSeconds.CACHE_1_HOUR);
                    if ("OK".equals(temp)) {
                        return true;
                    }
                } catch (Exception e) {
                    log.error("pool or redis object or command exception:" + e.getMessage(), e);
                } finally {
                    releaseResource(redis);
                    currentTime = System.currentTimeMillis();
                }
            }
        }
        return false;
    }

    public boolean tryLock(String key, int seconds, long timeoutMillis) {
        if (timeoutMillis <= 0L) {
            return tryLock(key, false);
        } else {
            ShardedJedis redis = null;
            long startTime = System.currentTimeMillis();
            long currentTime = startTime;
            while (currentTime - startTime < timeoutMillis) {
                try {
                    redis = getResource();
                    String temp = redis.set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX,
                            seconds > 0 ? seconds : TimeUnitSeconds.CACHE_1_HOUR);
                    if ("OK".equals(temp)) {
                        return true;
                    }
                } catch (Exception e) {
                    log.error("pool or redis object or command exception:" + e.getMessage(), e);
                } finally {
                    releaseResource(redis);
                    currentTime = System.currentTimeMillis();
                }
            }
            return false;
        }
    }

    public boolean lock(String key, boolean permanent) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            if (permanent) {
                Long temp = redis.setnx(key, "1");
                return (temp != null && temp.intValue() > 0);
            } else {
                String temp = redis.set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX, TimeUnitSeconds.CACHE_1_HOUR);
                return "OK".equals(temp);
            }
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return false;
    }

    public boolean lock(String key, int seconds) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            String temp = redis.set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX, seconds > 0 ? seconds : TimeUnitSeconds.CACHE_1_HOUR);
            return "OK".equals(temp);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return false;
    }

    public boolean isLocked(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.exists(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return false;
    }

    public Long unlock(String key) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.del(key);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public Long geoadd(String key, double longitude, double latitude, String member) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geoadd(key, longitude, latitude, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geoadd(key, memberCoordinateMap);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public Double geodist(String key, String member1, String member2) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geodist(key, member1, member2);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public Double geodist(String key, String member1, String member2, GeoUnit unit) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geodist(key, member1, member2, unit);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<String> geohash(String key, String... members) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geohash(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoCoordinate> geopos(String key, String... members) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geopos(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.georadius(key, longitude, latitude, radius, unit);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.georadius(key, longitude, latitude, radius, unit, param);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.georadiusByMember(key, member, radius, unit);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.georadiusByMember(key, member, radius, unit, param);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public Long geoadd(byte[] key, double longitude, double latitude, byte[] member) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geoadd(key, longitude, latitude, member);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public Long geoadd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geoadd(key, memberCoordinateMap);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public Double geodist(byte[] key, byte[] member1, byte[] member2) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geodist(key, member1, member2);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public Double geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geodist(key, member1, member2, unit);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<byte[]> geohash(byte[] key, byte[]... members) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geohash(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoCoordinate> geopos(byte[] key, byte[]... members) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.geopos(key, members);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude, double radius, GeoUnit unit) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.georadius(key, longitude, latitude, radius, unit);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadius(byte[] key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.georadius(key, longitude, latitude, radius, unit, param);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.georadiusByMember(key, member, radius, unit);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit, GeoRadiusParam param) {
        ShardedJedis redis = null;
        try {
            redis = getResource();
            return redis.georadiusByMember(key, member, radius, unit, param);
        } catch (Exception e) {
            log.error("pool or redis object or command exception:" + e.getMessage(), e);
        } finally {
            releaseResource(redis);
        }
        return null;
    }

    @Override
    public String loadCachePrefix() {
        return cachePrefix;
    }

    @Override
    public <T> T getCacheByKey(String key, Class<T> classType) {
        try {
            return CommonUtil.get(get(RedisHelperUtil.loadKey(classType, key, loadCachePrefix())), classType);
        } catch (Exception e) {
            log.error(classType.getName() + " Exception:" + e.getMessage(), e);
            return null;
        }
    }

    @Override
    public <T> boolean addCache(String key, T t, Class<T> classType, int seconds) {
        try {
            return "OK".equals(setex(RedisHelperUtil.loadKey(classType, key, loadCachePrefix()), seconds, CommonUtil.serialize(t)));
        } catch (Exception e) {
            log.error(classType.getName() + " Serializable no:" + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public <T> boolean deleteCache(String key, Class<T> classType) {
        try {
            return del(RedisHelperUtil.loadKey(classType, key, loadCachePrefix()), true) > 0L;
        } catch (Exception e) {
            log.error(classType.getName() + " :" + e.getMessage(), e);
            return false;
        }
    }
}
