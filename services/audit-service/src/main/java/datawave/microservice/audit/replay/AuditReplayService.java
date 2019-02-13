package datawave.microservice.audit.replay;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class AuditReplayService {

    private final ThreadPoolTaskExecutor auditReplayExecutor;
    private final ReplayStatusCache replayStatusCache;

    private final Map<String, RunningReplay> runningReplays = new HashMap<>();

    public AuditReplayService(ThreadPoolTaskExecutor auditReplayExecutor, ReplayStatusCache replayStatusCache) {
        this.auditReplayExecutor = auditReplayExecutor;
        this.replayStatusCache = replayStatusCache;
    }

    public String create(String path, String hdfsUri, long sendRate) {
        String id = UUID.randomUUID().toString();

        ReplayStatus status = replayStatusCache.create(id, path, hdfsUri, sendRate);

        return status.getId();
    }

    public String start(String id) {
        ReplayStatus status = replayStatusCache.retrieve(id);

        // if the state is 'created' or 'stopped', we can run the replay
        if (status != null && status.getState() == ReplayStatus.ReplayState.CREATED) {
            runningReplays.put(id, start(status));

            return "Started audit replay with id " + id;
        }

        return "Unable to start audit replay with id " + id;
    }

    private RunningReplay start(ReplayStatus status) {
        status.setState(ReplayStatus.ReplayState.RUNNING);
        replayStatusCache.update(status);

        RunningReplay replay = new RunningReplay(status);
        replay.setFuture(auditReplayExecutor.submit(new ReplayTask(status)));

        return replay;
    }

    public String startAll() {
        int replaysStarted = 0;
        List<ReplayStatus> replayStatuses = replayStatusCache.retrieveAll();
        if (replayStatuses != null) {
            for (ReplayStatus status : replayStatuses) {
                if (status.getState() == ReplayStatus.ReplayState.CREATED) {
                    RunningReplay replay = start(status);
                    runningReplays.put(status.getId(), replay);
                    replaysStarted++;
                }
            }
        }

        return replaysStarted + " replays started";
    }

    // get it from the cache every time so that the response is consistent across audit service pods
    public ReplayStatus status(String id) {
        return idleCheck(replayStatusCache.retrieve(id));
    }

    private ReplayStatus idleCheck(ReplayStatus status) {
        if (status != null) {
            // if the replay is RUNNING, and this hasn't been updated in the last 5 minutes, set the state to IDLE, and send out a stop request
            if (status.getState() == ReplayStatus.ReplayState.RUNNING && System.currentTimeMillis() - status.getLastUpdated().getTime() > TimeUnit.MINUTES.toMillis(5)) {
                status.setState(ReplayStatus.ReplayState.IDLE);
                replayStatusCache.update(status);
                stop(status.getId());
            }
        }

        return status;
    }

    // get if from the cache every time so that the response is consistent across audit service pods
    public List<ReplayStatus> statusAll(ReplayStatus.ReplayState state) {
        List<ReplayStatus> replayStatuses = replayStatusCache.retrieveAll();
        if (replayStatuses != null)
            replayStatuses = replayStatuses.stream().map(this::idleCheck).filter(replayStatus -> state == null || replayStatus.getState() == state).collect(Collectors.toList());
        return replayStatuses;
    }

    public String update(String id, long sendRate) {
        String response = "";

        // only update if the send rate is valid
        if (sendRate >= 0) {
            // pull the replay status from cache to ensure it exists
            ReplayStatus status = status(id);
            if (status != null) {

                // is the replay running?
                if (status.getState() == ReplayStatus.ReplayState.RUNNING) {

                    // if we own it, update it, otherwise fire an event to all of the audit services
                    RunningReplay replay = runningReplays.get(id);
                    if (replay != null) {
                        replay.getStatus().setSendRate(sendRate);
                    } else {
                        // TODO: Fire an update event
                    }
                } else {

                    // just update the cache if it's not running
                    status.setSendRate(sendRate);
                    replayStatusCache.update(status);
                }
            }
        }

        return response;
    }

    // post to stop a replay
    public String stop(String id) {
        String response = "";

        // if we own the replay, just stop it.  otherwise, if the id exists, send an event out to all audit services to stop the replay

        return response;
    }

    // post to stop all replays
    public String stopAll() {
        String response = "";

        // stop all of our replays.  then, send an event out to all audit services to stop all replays

        return response;
    }

    // post to cancel a replay
    public String cancel(String id) {
        String response = "";

        // if we own the replay, just cancel it.  otherwise, if the id exists, send an event out to all audit services to cancel a replay

        return response;
    }

    // post to cancel all replays
    public String cancelAll() {
        String response = "";

        // cancel all of our replays.  then, send an event out to all audit services to cancel all replays

        return response;
    }

    // post to resume a replay
    public String resume(String id) {
        String response = "";

        // if we own the stopped replay, just resume it.  otherwise, if the id exists, send an event out to all audit services to resume the replay

        return response;
    }

    // post to resume all replay
    public String resumeAll() {
        String response = "";

        // resume all of our stopped replays.  then, send an event out to all audit services to resume all replays

        return response;
    }
}
