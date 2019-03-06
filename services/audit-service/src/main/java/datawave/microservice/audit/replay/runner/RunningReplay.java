package datawave.microservice.audit.replay.runner;

import datawave.microservice.audit.replay.status.Status;

import java.util.concurrent.Future;

public class RunningReplay {
    
    final private Status status;
    private Future future;
    
    public RunningReplay(Status status, Future future) {
        this.status = status;
        this.future = future;
    }
    
    public Status getStatus() {
        return status;
    }
    
    public Future getFuture() {
        return future;
    }
}
