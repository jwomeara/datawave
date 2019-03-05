package org.springframework.cloud.bus.event;

import datawave.microservice.audit.replay.RemoteRequest;
import datawave.microservice.audit.replay.ReplayStatus;

import java.rmi.server.RemoteRef;
import java.util.List;

public class AuditReplayRemoteRequestEvent extends RemoteApplicationEvent {
    
    private final RemoteRequest remoteRequest;
    
    @SuppressWarnings("unused")
    public AuditReplayRemoteRequestEvent() {
        // this constructor is only for serialization/deserialization
        remoteRequest = null;
    }
    
    public AuditReplayRemoteRequestEvent(Object source, String originService, RemoteRequest remoteRequest) {
        this(source, originService, null, remoteRequest);
    }
    
    public AuditReplayRemoteRequestEvent(Object source, String originService, String destinationService, RemoteRequest remoteRequest) {
        super(source, originService, destinationService);
        this.remoteRequest = remoteRequest;
    }

    public RemoteRequest getRemoteRequest() {
        return remoteRequest;
    }
}
