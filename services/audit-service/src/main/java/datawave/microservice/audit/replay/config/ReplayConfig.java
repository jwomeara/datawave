package datawave.microservice.audit.replay.config;

import datawave.microservice.audit.replay.AuditReplayService;
import datawave.microservice.audit.replay.ReplayStatusCache;
import datawave.microservice.cached.CacheInspector;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableCaching
@ConditionalOnProperty(name = "audit.replay.enabled", havingValue = "true")
public class ReplayConfig {

    @Bean
    public ThreadPoolTaskExecutor auditReplayExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(7);
        executor.setMaxPoolSize(42);
        executor.setQueueCapacity(11);
        executor.setThreadNamePrefix("auditReplayExecutor-");
        executor.initialize();
        return executor;
    }

    @Bean
    public ReplayStatusCache replayStatusCache(CacheInspector cacheInspector) {
        return new ReplayStatusCache(cacheInspector);
    }

    @Bean
    public AuditReplayService auditReplayService(ThreadPoolTaskExecutor auditReplayExecutor, ReplayStatusCache replayStatusCache) {
        return new AuditReplayService(auditReplayExecutor, replayStatusCache);
    }

}
