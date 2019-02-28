package datawave.microservice.audit.replay.event.listener;

import datawave.microservice.audit.replay.ReplayStatus;
import datawave.microservice.audit.replay.ReplayStatusCache;
import datawave.microservice.audit.replay.RunningReplay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.AuditReplayUpdateEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Map;

import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.CANCELED;
import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.FAILED;
import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.FINISHED;
import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.STOPPED;

@Component
@ConditionalOnBusEnabled
@ConditionalOnProperty(name = "audit.replay.enabled", havingValue = "true")
public class AuditReplayUpdateEventListener implements ApplicationListener<AuditReplayUpdateEvent> {
    private Logger log = LoggerFactory.getLogger(getClass());
    private final ReplayStatusCache replayStatusCache;
    private final ServiceMatcher serviceMatcher;
    private final Map<String,RunningReplay> runningReplays;
    
    @Autowired
    public AuditReplayUpdateEventListener(ReplayStatusCache replayStatusCache, ServiceMatcher serviceMatcher, Map<String,RunningReplay> runningReplays) {
        this.replayStatusCache = replayStatusCache;
        this.serviceMatcher = serviceMatcher;
        this.runningReplays = runningReplays;
    }
    
    @Override
    public void onApplicationEvent(AuditReplayUpdateEvent event) {
        // Ignore events that this service instance published, since we publish from a place
        // that takes the same action we do here, and we don't want to repeat the work.
        if (serviceMatcher.isFromSelf(event)) {
            log.debug("Dropping {} since it is from us.", event);
            return;
        }
        
        for (ReplayStatus statusUpdate : event.getReplayStatuses()) {
            
            RunningReplay replay = runningReplays.get(statusUpdate.getId());
            if (replay != null) {
                
                boolean updateCache = false;
                
                // update the send rate if it has changed
                if (replay.getStatus().getSendRate() != statusUpdate.getSendRate()) {
                    replay.getStatus().setSendRate(statusUpdate.getSendRate());
                    updateCache = true;
                }
                
                // update the state if it has changed
                if (replay.getStatus().getState() != statusUpdate.getState()) {
                    if ((statusUpdate.getState() == STOPPED || statusUpdate.getState() == CANCELED)
                                    && (replay.getStatus().getState() != FINISHED && replay.getStatus().getState() != FAILED)) {
                        replay.getFuture().cancel(true);
                        replay.getStatus().setState(statusUpdate.getState());
                        runningReplays.remove(replay.getStatus().getId());
                        updateCache = true;
                    }
                }
                
                if (updateCache)
                    replayStatusCache.update(replay.getStatus());
                
            }
        }
    }
}
