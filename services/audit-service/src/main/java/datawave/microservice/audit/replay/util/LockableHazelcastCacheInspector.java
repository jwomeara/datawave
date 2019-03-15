package datawave.microservice.audit.replay.util;

import com.hazelcast.core.IMap;
import datawave.microservice.cached.CacheInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A {@link CacheInspector} that is capable of inspecting a {@link com.hazelcast.spring.cache.HazelcastCache}.
 */
class LockableHazelcastCacheInspector implements CacheInspector, LockableCacheInspector {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CacheManager cacheManager;
    
    public LockableHazelcastCacheInspector(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }
    
    @Override
    public <T> T list(String cacheName, Class<T> cacheObjectType, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        return cache.get(key, cacheObjectType);
    }
    
    @Override
    public <T> List<? extends T> listAll(String cacheName, Class<T> cacheObjectType) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            // @formatter:off
            return imap.values().stream()
                    .map(cacheObjectType::cast)
                    .collect(Collectors.toList());
            // @formatter:on
        } else {
            logger.error("Native cache should be an IMap, but instead is {}.", cache.getNativeCache().getClass());
            return Collections.emptyList();
        }
    }
    
    @Override
    public <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            // @formatter:off
            return imap.values(e -> String.valueOf(e.getKey()).contains(substring)).stream()
                    .map(cacheObjectType::cast)
                    .collect(Collectors.toList());
            // @formatter:on
        } else {
            logger.error("Native cache should be an IMap, but instead is {}.", cache.getNativeCache().getClass());
            return Collections.emptyList();
        }
    }
    
    @Override
    public <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            Set<Object> keysToRemove = imap.keySet(e -> String.valueOf(e.getKey()).contains(substring));
            keysToRemove.forEach(imap::remove);
            return keysToRemove.size();
        } else {
            logger.error("Native cache should be an IMap, but instead is {}.", cache.getNativeCache().getClass());
            return 0;
        }
    }

    @Override
    public void lock(String cacheName, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            imap.lock(key);
        }
    }

    @Override
    public void lock(String cacheName, String key, long leaseTime, TimeUnit leaseTimeUnit) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            imap.lock(key, leaseTime, leaseTimeUnit);
        }
    }

    @Override
    public boolean tryLock(String cacheName, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            return imap.tryLock(key);
        }
        return false;
    }

    @Override
    public boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit) throws InterruptedException {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            return imap.tryLock(key, waitTime, waitTimeUnit);
        }
        return false;
    }

    @Override
    public boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit, long leaseTime, TimeUnit leaseTimeUnit) throws InterruptedException {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            return imap.tryLock(key, waitTime, waitTimeUnit, leaseTime, leaseTimeUnit);
        }
        return false;
    }

    @Override
    public void unlock(String cacheName, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            imap.unlock(key);
        }
    }

    @Override
    public void forceUnlock(String cacheName, String key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache.getNativeCache() instanceof IMap) {
            @SuppressWarnings("unchecked")
            IMap<Object,Object> imap = (IMap<Object,Object>) cache.getNativeCache();
            imap.forceUnlock(key);
        }
    }
}
