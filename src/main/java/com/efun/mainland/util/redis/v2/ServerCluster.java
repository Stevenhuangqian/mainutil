package com.efun.mainland.util.redis.v2;

import com.efun.mainland.util.CommonUtil;
import com.efun.mainland.util.cache.TimeUnitSeconds;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by Efun on 2016-10-31.
 */
public class ServerCluster extends JedisCluster implements RedisService {

    private static final Logger log = LoggerFactory.getLogger(ServerCluster.class);

    private final Timer timer = new Timer("ServerCluster-delay-clear-delete-cache-timer");

    private long intervalTime = 5 * 60 * 1000;
    private String cachePrefix = "";
    /**
     * 为了避免数据库主从同步延迟导致的问题，需要再次删除缓存key值的队列key
     */
    private String CACHE_KEYS_QUEUE_STRING;
    private byte[] CACHE_KEYS_QUEUE_BYTE;

    public ServerCluster(Set<HostAndPort> nodes) {
        this(nodes, DEFAULT_TIMEOUT);
    }

    public ServerCluster(Set<HostAndPort> nodes, int timeout) {
        this(nodes, timeout, DEFAULT_MAX_REDIRECTIONS);
    }

    public ServerCluster(Set<HostAndPort> nodes, int timeout, int maxRedirections) {
        this(nodes, timeout, maxRedirections, new GenericObjectPoolConfig());
    }

    public ServerCluster(Set<HostAndPort> nodes, final GenericObjectPoolConfig poolConfig) {
        this(nodes, DEFAULT_TIMEOUT, new GenericObjectPoolConfig());
    }

    public ServerCluster(Set<HostAndPort> nodes, int timeout, final GenericObjectPoolConfig poolConfig) {
        this(nodes, timeout,DEFAULT_MAX_REDIRECTIONS, poolConfig);
    }

    public ServerCluster(Set<HostAndPort> redisClusterNode, int timeout, int maxRedirections,
                         final GenericObjectPoolConfig poolConfig) {
        this(redisClusterNode, timeout,timeout, maxRedirections, poolConfig);
    }

    public ServerCluster(Set<HostAndPort> redisClusterNode, int connectionTimeout, int soTimeout, int maxRedirections,
                         final GenericObjectPoolConfig poolConfig) {
        super(redisClusterNode, connectionTimeout, soTimeout, maxRedirections, poolConfig);
        log.info("redisClusterNodes={},maxRedirections={}",redisClusterNode,maxRedirections);
        init();
    }

    public ServerCluster(String redisClusterNodes, int connectionTimeout, int soTimeout, int maxRedirections,
                         final GenericObjectPoolConfig poolConfig) {
        this(RedisHelperUtil.string2HostAndPortSet(redisClusterNodes), connectionTimeout, soTimeout, maxRedirections, poolConfig);
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
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
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

    protected void destroy() throws IOException {
        super.close();
    }

    public boolean isServerCluster() {
        return true;
    }

    @Override
    public Long del(byte[] key, boolean delay) {
        if (delay) {
            zadd(CACHE_KEYS_QUEUE_BYTE, System.currentTimeMillis(), key);
        }
        return del(key);
    }

    @Override
    public Long del(String key, boolean delay) {
        if (delay) {
            zadd(CACHE_KEYS_QUEUE_STRING, System.currentTimeMillis(), key);
        }
        return del(key);
    }

    public boolean tryLock(String key, boolean permanent) {
        boolean result = false;
        while (!result) {
            if (permanent) {
                Long temp = setnx(key, "1");
                if (temp != null && temp.intValue() > 0) {
                    result = true;
                }
            } else {
                String temp = set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX,
                        TimeUnitSeconds.CACHE_1_HOUR);
                result = "OK".equals(temp);
            }
        }
        return result;
    }

    public boolean tryLock(String key, long timeoutMillis) {
        if (timeoutMillis <= 0L) {
            return tryLock(key, false);
        } else {
            boolean result = false;
            long startTime = System.currentTimeMillis();
            long currentTime = startTime;
            while ((!result) && (currentTime - startTime < timeoutMillis)) {
                String temp = set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX,
                        TimeUnitSeconds.CACHE_1_HOUR);
                result = "OK".equals(temp);
                currentTime = System.currentTimeMillis();
            }

            return result;
        }
    }

    public boolean tryLock(String key, int seconds, long timeoutMillis) {
        if (timeoutMillis <= 0L) {
            return tryLock(key, false);
        } else {
            boolean result = false;
            long startTime = System.currentTimeMillis();
            long currentTime = startTime;
            while ((!result) && (currentTime - startTime < timeoutMillis)) {
                String temp = set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX,
                        seconds > 0 ? seconds : TimeUnitSeconds.CACHE_1_HOUR);
                result = "OK".equals(temp);
                currentTime = System.currentTimeMillis();
            }

            return result;
        }
    }

    public boolean lock(String key, boolean permanent) {
        boolean result = false;
        if (permanent) {
            Long temp = setnx(key, "1");
            if (temp != null && temp.intValue() > 0) {
                result = true;
            }
        } else {
            String temp = set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX,
                    TimeUnitSeconds.CACHE_1_HOUR);
            result = "OK".equals(temp);
        }

        return result;
    }

    public boolean lock(String key, int seconds) {
        String temp = set(key, "1", RedisHelperUtil.NX, RedisHelperUtil.EX, seconds > 0 ? seconds : TimeUnitSeconds.CACHE_1_HOUR);
        return "OK".equals(temp);
    }

    public boolean isLocked(String key) {
        return exists(key);
    }

    public Long unlock(String key) {
        return del(key);
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
    public <T> boolean addCache(String key, T t, Class<T> classType, int seconds, boolean force) {
        try {
            if (force){
                return "OK".equals(setex(RedisHelperUtil.loadKey(classType, key, loadCachePrefix()), seconds, CommonUtil.serialize(t)));
            }else{
                return "OK".equals(set(RedisHelperUtil.loadKey(classType, key, loadCachePrefix()),CommonUtil.serialize(t),CommonUtil.stringToByte(RedisHelperUtil.NX),CommonUtil.stringToByte(RedisHelperUtil.EX),seconds));
            }
        } catch (Exception e) {
            log.error(classType.getName() + " Serializable no:" + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public <T> boolean deleteCache(String key, Class<T> classType, boolean delay) {
        try {
            return del(RedisHelperUtil.loadKey(classType, key, loadCachePrefix()), delay) > 0L;
        } catch (Exception e) {
            log.error(classType.getName() + " :" + e.getMessage(), e);
            return false;
        }
    }
}
