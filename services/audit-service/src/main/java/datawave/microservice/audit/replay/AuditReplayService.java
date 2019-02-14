package datawave.microservice.audit.replay;

import org.apache.hadoop.conf.Configuration;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.AuditReplayUpdateEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.*;

public class AuditReplayService {

    private final ThreadPoolTaskExecutor auditReplayExecutor;
    private final ReplayStatusCache replayStatusCache;
    private final Map<String, RunningReplay> runningReplays;
    private final ApplicationContext appCtx;
    private final BusProperties busProperties;

    public AuditReplayService(ThreadPoolTaskExecutor auditReplayExecutor, ReplayStatusCache replayStatusCache, Map<String, RunningReplay> runningReplays, ApplicationContext appCtx, BusProperties busProperties) {
        this.auditReplayExecutor = auditReplayExecutor;
        this.replayStatusCache = replayStatusCache;
        this.runningReplays = runningReplays;
        this.appCtx = appCtx;
        this.busProperties = busProperties;
    }

    public String create(String path, String hdfsUri, long sendRate) {
        String id = UUID.randomUUID().toString();

        ReplayStatus status = replayStatusCache.create(id, path, hdfsUri, sendRate);

        return status.getId();
    }

    public String start(Configuration config, String id) {
        ReplayStatus status = status(id);

        // if the state is 'created' or 'stopped', we can run the replay
        if (status != null) {
            if (status.getState() == CREATED) {
                runningReplays.put(id, start(config, status));
            } else {
                throw new RuntimeException("Cannot start audit replay with state " + status.getState());
            }
        } else {
            throw new RuntimeException("No audit replay found with id " + id);
        }

        return "Started audit replay with id " + id;
    }

    public String createAndStart(Configuration config, String path, String hdfsUri, long sendRate) {
        String id = UUID.randomUUID().toString();

        runningReplays.put(id, start(config, replayStatusCache.create(id, path, hdfsUri, sendRate)));

        return "Started audit replay with id " + id;
    }

    private RunningReplay start(Configuration config, ReplayStatus status) {
        status.setState(RUNNING);
        replayStatusCache.update(status);

        RunningReplay replay = new RunningReplay(status);
        ReplayTask replayTask = null;
        try {
            replayTask = new ReplayTask(config, status);
        } catch (Exception e) {
           throw new RuntimeException("Unable to create replay task");
        }

        replay.setFuture(auditReplayExecutor.submit(replayTask));

        return replay;
    }

    public String startAll(Configuration config) {
        int replaysStarted = 0;
        List<ReplayStatus> replayStatuses = replayStatusCache.retrieveAll();
        if (replayStatuses != null) {
            for (ReplayStatus status : replayStatuses) {
                if (status.getState() == CREATED) {
                    RunningReplay replay = start(config, status);
                    runningReplays.put(status.getId(), replay);
                    replaysStarted++;
                }
            }
        }

        return replaysStarted + " audit replays started";
    }

    // get it from the cache every time so that the response is consistent across audit service pods
    public ReplayStatus status(String id) {
        ReplayStatus status = replayStatusCache.retrieve(id);
        if (status != null)
            return idleCheck(status);
        else
            return null;
    }

    private ReplayStatus idleCheck(ReplayStatus status) {
        // if the replay is RUNNING, and this hasn't been updated in the last 5 minutes, set the state to IDLE, and send out a stop request
        if (status.getState() == RUNNING && System.currentTimeMillis() - status.getLastUpdated().getTime() > TimeUnit.MINUTES.toMillis(5)) {
            status.setState(IDLE);
            replayStatusCache.update(status);
            stop(status.getId());
        }

        return status;
    }

    // get if from the cache every time so that the response is consistent across audit service pods
    public List<ReplayStatus> statusAll() {
        List<ReplayStatus> replayStatuses = replayStatusCache.retrieveAll();
        if (replayStatuses != null)
            replayStatuses = replayStatuses.stream().map(this::idleCheck).collect(Collectors.toList());
        return replayStatuses;
    }

    public String update(String id, long sendRate) {

        // only update if the send rate is valid
        if (sendRate >= 0) {
            // pull the replay status from cache to ensure it exists
            ReplayStatus status = status(id);
            if (status != null) {

                // is the replay running?
                if (status.getState() == RUNNING) {

                    // if we own it, update it. otherwise fire an event to all of the audit services
                    RunningReplay replay = runningReplays.get(id);
                    if (replay != null) {

                        replay.getStatus().setSendRate(sendRate);
                        replayStatusCache.update(replay.getStatus());
                    } else {

                        status.setSendRate(sendRate);
                        appCtx.publishEvent(new AuditReplayUpdateEvent(this, busProperties.getId(), Collections.singletonList(status)));
                    }
                } else {

                    // just update the cache if it's not running
                    status.setSendRate(sendRate);
                    replayStatusCache.update(status);
                }
            } else {
                throw new RuntimeException("No audit replay found with id " + id);
            }
        } else {
            throw new RuntimeException("Send rate must be >= 0");
        }

        return "Updated audit replay with id " + id;
    }

    // post to stop a replay
    public String stop(String id) {

        ReplayStatus status = status(id);
        if (status != null) {
            if (!stop(status))
                throw new RuntimeException("Cannot stop audit replay with id " + id);
        } else {
            throw new RuntimeException("No audit replay found with id " + id);
        }

        return "Stopped audit replay with id " + id;
    }

    private boolean stop(ReplayStatus status) {
        // if we own the replay, just stop it.  otherwise, if the id exists, send an event out to all audit services to stop the replay

        // is the replay running?
        if (status.getState() == RUNNING) {

            // if we own it, stop it.  otherwise, fire an event to all of the audit services
            RunningReplay replay = runningReplays.get(status.getId());
            if (replay != null) {

                replay.getStatus().setState(STOPPED);
                replayStatusCache.update(replay.getStatus());

                replay.getFuture().cancel(true);
                runningReplays.remove(status.getId());
            } else {

                status.setState(STOPPED);
                appCtx.publishEvent(new AuditReplayUpdateEvent(this, busProperties.getId(), Collections.singletonList(status)));
            }
        } else {

            return false;
        }

        return true;
    }

    // post to stop all replays
    public String stopAll() {
        int replaysStopped = 0;

        // stop all of our replays.  then, send an event out to all audit services to stop all replays
        List<ReplayStatus> replayStatuses = statusAll().stream().filter(status -> status.getState() == RUNNING).collect(Collectors.toList());
        for (ReplayStatus status : replayStatuses)
            if (stop(status))
                replaysStopped++;

        return replaysStopped + " audit replays stopped";
    }

    // post to cancel a replay
    public String cancel(String id) {

        ReplayStatus status = status(id);
        if (status != null) {
            if (!cancel(status))
                throw new RuntimeException("Cannot cancel audit replay with id " + id);
        } else {
            throw new RuntimeException("No audit replay found with id " + id);
        }

        return "Canceled audit replay with id " + id;
    }

    private boolean cancel(ReplayStatus status) {

        // if we own the replay, just cancel it.  otherwise, if the id exists, send an event out to all audit services to cancel the replay
        if (status.getState() != CANCELED && status.getState() != FINISHED && status.getState() != FAILED) {

            // is the replay running?
            if (status.getState() == RUNNING) {

                // if we own it, stop it.  otherwise, fire an event to all of the audit services
                RunningReplay replay = runningReplays.get(status.getId());
                if (replay != null) {

                    replay.getStatus().setState(CANCELED);
                    replayStatusCache.update(replay.getStatus());

                    replay.getFuture().cancel(true);
                    runningReplays.remove(status.getId());
                } else {

                    status.setState(CANCELED);
                    appCtx.publishEvent(new AuditReplayUpdateEvent(this, busProperties.getId(), Collections.singletonList(status)));
                }
            } else {
                // just update the cache if it's not running
                status.setState(CANCELED);
                replayStatusCache.update(status);
            }
        }

        return true;
    }

    // post to cancel all replays
    public String cancelAll() {
        int replaysCanceled = 0;

        // cancel all of our replays.  then, send an event out to all audit services to cancel all replays
        List<ReplayStatus> replayStatuses = statusAll().stream().filter(status -> status.getState() != CANCELED && status.getState() != FINISHED && status.getState() != FAILED).collect(Collectors.toList());
        for (ReplayStatus status : replayStatuses)
            if (cancel(status))
                replaysCanceled++;

        return replaysCanceled + " audit replays canceled";
    }

    // post to resume a replay
    public String resume(Configuration config, String id) {

        ReplayStatus status = status(id);
        if (status != null) {
            if (!resume(config, status))
                throw new RuntimeException("Cannot resume audit replay with id " + id);
        } else {
            throw new RuntimeException("No audit replay found with id " + id);
        }

        return "Resumed audit replay with id " + id;
    }

    private boolean resume(Configuration config, ReplayStatus status) {

        // if the audit replay is idle or stopped, start it
        if (status.getState() == IDLE || status.getState() == STOPPED) {

            runningReplays.put(status.getId(), start(config, status));
        } else {

            return false;
        }

        return true;
    }

    // post to resume all replay
    public String resumeAll(Configuration config) {
        int replaysResumed = 0;

        // resume all of our replays
        List<ReplayStatus> replayStatuses = statusAll().stream().filter(status -> status.getState() == IDLE && status.getState() == STOPPED).collect(Collectors.toList());
        for (ReplayStatus status : replayStatuses) {
            runningReplays.put(status.getId(), start(config, status));
            replaysResumed++;
        }

        return replaysResumed + " audit replays resumed";
    }
}
