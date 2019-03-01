package datawave.microservice.audit.replay.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;

@ConfigurationProperties(prefix = "audit.replay")
public class ReplayProperties {
    private boolean enabled;
    private long idleTimeoutMillis = TimeUnit.SECONDS.toMillis(30);
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public long getIdleTimeoutMillis() {
        return idleTimeoutMillis;
    }
    
    public void setIdleTimeoutMillis(long idleTimeoutMillis) {
        this.idleTimeoutMillis = idleTimeoutMillis;
    }
}
