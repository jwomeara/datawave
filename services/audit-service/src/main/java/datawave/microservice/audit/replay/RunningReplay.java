package datawave.microservice.audit.replay;

import java.util.concurrent.Future;

public class RunningReplay {
    
    final private ReplayStatus status;
    private Future future;
    
    public RunningReplay(ReplayStatus status) {
        this.status = status;
    }
    
    public ReplayStatus getStatus() {
        return status;
    }
    
    public Future getFuture() {
        return future;
    }
    
    public void setFuture(Future future) {
        this.future = future;
    }
    
    // TODO: Add safe stop method
}
