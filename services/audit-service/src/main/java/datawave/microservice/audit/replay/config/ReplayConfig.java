package datawave.microservice.audit.replay.config;

import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.microservice.audit.replay.status.StatusCache;
import datawave.microservice.audit.replay.util.LockableCacheInspector;
import datawave.microservice.audit.replay.util.LockableConcurrentMapCacheInspector;
import datawave.microservice.audit.replay.util.LockableHazelcastCacheInspector;
import datawave.microservice.cached.CacheInspector;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableCaching
@EnableConfigurationProperties(ReplayProperties.class)
@ConditionalOnProperty(name = "audit.replay.enabled", havingValue = "true")
public class ReplayConfig {
    
    @RefreshScope
    @Bean
    public ThreadPoolTaskExecutor auditReplayExecutor(ReplayProperties replayProperties) {
        ReplayProperties.ExecutorProperties executorProperties = replayProperties.getExecutor();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(executorProperties.getCorePoolSize());
        executor.setMaxPoolSize(executorProperties.getMaxPoolSize());
        executor.setQueueCapacity(executorProperties.getQueueCapacity());
        executor.setThreadNamePrefix(executorProperties.getThreadNamePrefix());
        executor.initialize();
        return executor;
    }
    
    @Bean
    public StatusCache replayStatusCache(CacheInspector cacheInspector, CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector = null;
        if (cacheManager instanceof ConcurrentMapCacheManager)
            lockableCacheInspector = new LockableConcurrentMapCacheInspector((ConcurrentMapCacheManager) cacheManager);
        else if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        return new StatusCache(lockableCacheInspector);
    }
}
