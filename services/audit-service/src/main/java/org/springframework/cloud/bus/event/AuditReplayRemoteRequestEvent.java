package org.springframework.cloud.bus.event;

import datawave.microservice.audit.replay.remote.Request;

public class AuditReplayRemoteRequestEvent extends RemoteApplicationEvent {
    
    private final Request request;
    
    @SuppressWarnings("unused")
    public AuditReplayRemoteRequestEvent() {
        // this constructor is only for serialization/deserialization
        request = null;
    }
    
    public AuditReplayRemoteRequestEvent(Object source, String originService, Request request) {
        this(source, originService, null, request);
    }
    
    public AuditReplayRemoteRequestEvent(Object source, String originService, String destinationService, Request request) {
        super(source, originService, destinationService);
        this.request = request;
    }
    
    public Request getRequest() {
        return request;
    }
}
