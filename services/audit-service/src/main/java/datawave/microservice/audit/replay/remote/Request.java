package datawave.microservice.audit.replay.remote;

import static datawave.microservice.audit.replay.remote.Request.Method.CANCEL;
import static datawave.microservice.audit.replay.remote.Request.Method.CANCEL_ALL;
import static datawave.microservice.audit.replay.remote.Request.Method.STOP;
import static datawave.microservice.audit.replay.remote.Request.Method.STOP_ALL;
import static datawave.microservice.audit.replay.remote.Request.Method.UPDATE;

public class Request {
    
    public enum Method {
        UPDATE, STOP, STOP_ALL, CANCEL, CANCEL_ALL
    }
    
    private final Method method;
    private final String id;
    
    private Request(Method method) {
        this(method, null);
    }
    
    private Request(Method method, String id) {
        this.method = method;
        this.id = id;
    }
    
    public Method getMethod() {
        return method;
    }
    
    public String getId() {
        return id;
    }
    
    @Override
    public String toString() {
        return "Remote Request: method=" + method + ", id=" + id;
    }
    
    public static class UpdateRequest extends Request {
        final private long sendRate;
        
        private UpdateRequest(String id, long sendRate) {
            super(UPDATE, id);
            this.sendRate = sendRate;
        }
        
        public long getSendRate() {
            return sendRate;
        }
        
        @Override
        public String toString() {
            return super.toString() + ", sendRate=" + sendRate;
        }
    }
    
    public static Request update(String id, long sendRate) {
        return new UpdateRequest(id, sendRate);
    }
    
    public static Request stop(String id) {
        return new Request(STOP, id);
    }
    
    public static Request stopAll() {
        return new Request(STOP_ALL);
    }
    
    public static Request cancel(String id) {
        return new Request(CANCEL, id);
    }
    
    public static Request cancelAll() {
        return new Request(CANCEL_ALL);
    }
}
