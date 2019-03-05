package datawave.microservice.audit.replay.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;

@ConfigurationProperties(prefix = "audit.replay")
public class ReplayProperties {
    private boolean enabled;
    private boolean publishEventsEnabled = true;
    private long idleTimeoutMillis = TimeUnit.SECONDS.toMillis(30);
    private long stopGracePeriodMillis = 500L;
    
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
}
