package datawave.microservice.audit.replay.event.listener;

import datawave.microservice.audit.replay.ReplayStatusCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.AuditReplayUpdateEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnBusEnabled
public class AuditReplayUpdateEventListener implements ApplicationListener<AuditReplayUpdateEvent> {
    private Logger log = LoggerFactory.getLogger(getClass());
    private final ReplayStatusCache replayStatusCache;
    private final ServiceMatcher serviceMatcher;

    @Autowired
    public AuditReplayUpdateEventListener(ReplayStatusCache replayStatusCache, ServiceMatcher serviceMatcher) {
        this.replayStatusCache = replayStatusCache;
        this.serviceMatcher = serviceMatcher;
    }
    
    @Override
    public void onApplicationEvent(AuditReplayUpdateEvent event) {
        // Ignore events that this service instance published, since we publish from a place
        // that takes the same action we do here, and we don't want to repeat the work.
        if (serviceMatcher.isFromSelf(event)) {
            log.debug("Dropping {} since it is from us.", event);
            return;
        }
        switch (event.getEvictionType()) {
            case FULL:
                log.info("Received event to evict all users from the cache.");
                userService.evictAll();
                break;
            case PARTIAL:
                log.info("Received event to evict users matching " + event.getSubstring() + " from the cache.");
                userService.evictMatching(event.getSubstring());
                break;
            case USER:
                log.info("Received event to evict user " + event.getSubstring() + " from the cache.");
                userService.evict(event.getSubstring());
                break;
        }
    }
}
