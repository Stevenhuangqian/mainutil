package com.efun.mainland.util.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("rawtypes")
public class CacheEntityUtil {
    private final static Logger logger = LoggerFactory.getLogger(CacheEntityUtil.class);
    private final static Timer timer = new Timer("delay-clear-delete-cache-timer");
    private final static long time = 5 * 60 * 1000;
    private final static ConcurrentHashMap<String, CacheEntity> map = new ConcurrentHashMap<String, CacheEntity>();

    static {
        try {
            timer.scheduleAtFixedRate(new TimerTask() {
                public void run() {
                    try {
                        long startTime = System.currentTimeMillis();
                        Set<Entry<String, CacheEntity>> set = map.entrySet();
                        int before = map.size();
                        for (Entry<String, CacheEntity> keyValue : set) {
                            CacheEntity temp = keyValue.getValue();
                            if (temp == null || temp.ttl() <= 0L) {
                                map.remove(keyValue.getKey());
                                logger.debug("timeout:key={}", keyValue.getKey());
                            }
                        }
                        int after = map.size();
                        logger.info("memory cache size:before={},after={},time={}milliseconds", before, after,
                                (System.currentTimeMillis() - startTime));
                    } catch (Throwable e) {
                        logger.error("Throwable:" + e.getMessage(), e);
                    }
                }
            }, 1000 * 60, time);
        } catch (Throwable e) {
        }
    }

    public static final boolean containsKey(String key) {
        if (key != null) {
            return map.containsKey(key);
        } else {
            return false;
        }
    }

    public static final <T> void setCache(String key, CacheEntity<T> cacheEntity) {
        if (cacheEntity != null && key != null) {
            map.put(key, cacheEntity);
            logger.debug("set cache:key={},ttl={}milliseconds", key, cacheEntity.ttl());
        }
    }

    public static final void deleteCache(String key) {
        if (key != null) {
            map.remove(key);
            logger.debug("delete cache:key={}", key);
        }
    }

    public static final <T> CacheEntity<T> getCache(String key) {
        CacheEntity<T> cacheEntity = null;
        if (key != null) {
            cacheEntity = map.get(key);
            if (cacheEntity != null) {
                long ttl = cacheEntity.ttl();
                if (cacheEntity.ttl() <= 0L) {
                    map.remove(key);
                    cacheEntity = null;
                    logger.debug("timeout:key={}", key);
                } else {
                    logger.debug("get cache success:key={},ttl={}milliseconds", key, ttl);
                }
            }
        }
        return cacheEntity;
    }
}
