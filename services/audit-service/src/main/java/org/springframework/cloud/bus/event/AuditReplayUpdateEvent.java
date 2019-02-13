package org.springframework.cloud.bus.event;

import datawave.microservice.audit.replay.ReplayStatus;

public class AuditReplayUpdateEvent extends RemoteApplicationEvent {

    private final ReplayStatus replayStatus;

    @SuppressWarnings("unused")
    public AuditReplayUpdateEvent() {
        // this constructor is only for serialization/deserialization
        replayStatus = null;
    }

    public AuditReplayUpdateEvent(Object source, String originService, ReplayStatus replayStatus) {
        this(source, originService, null, replayStatus);
    }

    public AuditReplayUpdateEvent(Object source, String originService, String destinationService, ReplayStatus replayStatus) {
        super(source, originService, destinationService);
        this.replayStatus = replayStatus;
    }

    public ReplayStatus getReplayStatus() {
        return replayStatus;
    }
}
