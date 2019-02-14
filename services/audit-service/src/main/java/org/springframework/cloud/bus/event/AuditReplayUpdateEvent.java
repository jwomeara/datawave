package org.springframework.cloud.bus.event;

import datawave.microservice.audit.replay.ReplayStatus;

import java.util.List;

public class AuditReplayUpdateEvent extends RemoteApplicationEvent {

    private final List<ReplayStatus> replayStatuses;

    @SuppressWarnings("unused")
    public AuditReplayUpdateEvent() {
        // this constructor is only for serialization/deserialization
        replayStatuses = null;
    }

    public AuditReplayUpdateEvent(Object source, String originService, List<ReplayStatus> replayStatuses) {
        this(source, originService, null, replayStatuses);
    }

    public AuditReplayUpdateEvent(Object source, String originService, String destinationService, List<ReplayStatus> replayStatuses) {
        super(source, originService, destinationService);
        this.replayStatuses = replayStatuses;
    }

    public List<ReplayStatus> getReplayStatuses() {
        return replayStatuses;
    }
}
