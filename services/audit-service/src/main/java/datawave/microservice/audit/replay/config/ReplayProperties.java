package datawave.microservice.audit.replay.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;

@ConfigurationProperties(prefix = "audit.replay")
public class ReplayProperties {
    private boolean enabled;
    private boolean publishEventsEnabled = true;
    private long idleTimeoutMillis = TimeUnit.SECONDS.toMillis(30);
    private long stopGracePeriodMillis = 500L;
    
    private ExecutorProperties executor = new ExecutorProperties();
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public boolean isPublishEventsEnabled() {
        return publishEventsEnabled;
    }
    
    public void setPublishEventsEnabled(boolean publishEventsEnabled) {
        this.publishEventsEnabled = publishEventsEnabled;
    }
    
    public long getIdleTimeoutMillis() {
        return idleTimeoutMillis;
    }
    
    public void setIdleTimeoutMillis(long idleTimeoutMillis) {
        this.idleTimeoutMillis = idleTimeoutMillis;
    }
    
    public long getStopGracePeriodMillis() {
        return stopGracePeriodMillis;
    }
    
    public void setStopGracePeriodMillis(long stopGracePeriodMillis) {
        this.stopGracePeriodMillis = stopGracePeriodMillis;
    }
    
    public ExecutorProperties getExecutor() {
        return executor;
    }
    
    public void setExecutor(ExecutorProperties executor) {
        this.executor = executor;
    }
    
    public static class ExecutorProperties {
        private int corePoolSize = 0;
        private int maxPoolSize = 5;
        private int queueCapacity = 0;
        private String threadNamePrefix = "auditReplayExecutor-";
        
        public int getCorePoolSize() {
            return corePoolSize;
        }
        
        public void setCorePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
        }
        
        public int getMaxPoolSize() {
            return maxPoolSize;
        }
        
        public void setMaxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
        }
        
        public int getQueueCapacity() {
            return queueCapacity;
        }
        
        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }
        
        public String getThreadNamePrefix() {
            return threadNamePrefix;
        }
        
        public void setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
        }
    }
}
