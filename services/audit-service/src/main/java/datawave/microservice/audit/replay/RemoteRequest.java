package datawave.microservice.audit.replay;

import static datawave.microservice.audit.replay.RemoteRequest.RequestMethod.CANCEL;
import static datawave.microservice.audit.replay.RemoteRequest.RequestMethod.CANCEL_ALL;
import static datawave.microservice.audit.replay.RemoteRequest.RequestMethod.STOP;
import static datawave.microservice.audit.replay.RemoteRequest.RequestMethod.STOP_ALL;
import static datawave.microservice.audit.replay.RemoteRequest.RequestMethod.UPDATE;

public class RemoteRequest {

    public enum RequestMethod {
        UPDATE,
        STOP,
        STOP_ALL,
        CANCEL,
        CANCEL_ALL
    }

    private final RequestMethod method;
    private final String id;

    private RemoteRequest(RequestMethod method) {
        this(method, null);
    }

    private RemoteRequest(RequestMethod method, String id) {
        this.method = method;
        this.id = id;
    }

    public RequestMethod getMethod() {
        return method;
    }

    public String getId() {
        return id;
    }

    public static class UpdateRequest extends RemoteRequest {
        final private long sendRate;

        private UpdateRequest(String id, long sendRate) {
            super(UPDATE, id);
            this.sendRate = sendRate;
        }

        public long getSendRate() {
            return sendRate;
        }
    }

    public static RemoteRequest update(String id, long sendRate) {
        return new UpdateRequest(id, sendRate);
    }

    public static RemoteRequest stop(String id) {
        return new RemoteRequest(STOP, id);
    }

    public static RemoteRequest stopAll() {
        return new RemoteRequest(STOP_ALL);
    }

    public static RemoteRequest cancel(String id) {
        return new RemoteRequest(CANCEL, id);
    }

    public static RemoteRequest cancelAll() {
        return new RemoteRequest(CANCEL_ALL);
    }
}
