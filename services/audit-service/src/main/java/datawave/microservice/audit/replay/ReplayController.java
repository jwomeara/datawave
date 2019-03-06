package datawave.microservice.audit.replay;

import datawave.microservice.audit.AuditController;
import datawave.microservice.audit.config.AuditProperties;
import datawave.microservice.audit.replay.config.ReplayProperties;
import datawave.microservice.audit.replay.remote.Request;
import datawave.microservice.audit.replay.runner.ReplayTask;
import datawave.microservice.audit.replay.runner.RunningReplay;
import datawave.microservice.audit.replay.status.Status;
import datawave.microservice.audit.replay.status.StatusCache;
import datawave.webservice.common.audit.AuditParameters;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.AuditReplayRemoteRequestEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.http.MediaType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static datawave.microservice.audit.replay.status.Status.ReplayState.CANCELED;
import static datawave.microservice.audit.replay.status.Status.ReplayState.CREATED;
import static datawave.microservice.audit.replay.status.Status.ReplayState.FAILED;
import static datawave.microservice.audit.replay.status.Status.ReplayState.FINISHED;
import static datawave.microservice.audit.replay.status.Status.ReplayState.IDLE;
import static datawave.microservice.audit.replay.status.Status.ReplayState.RUNNING;
import static datawave.microservice.audit.replay.status.Status.ReplayState.STOPPED;

/**
 * The ReplayController presents the REST endpoints for audit replay.
 * <p>
 * Before returning success to the caller, the audit controller will verify that the audit message was successfully passed to our messaging infrastructure.
 * Also, if configured, the audit controller will verify that the message passing infrastructure is healthy before returning successfully to the user. If the
 * message passing infrastructure is unhealthy, or if we can't verify that the message was successfully passed to our messaging infrastructure, a 500 Internal
 * Server Error will be returned to the caller.
 */
@RestController
@RolesAllowed({"Administrator", "JBossAdministrator"})
@RequestMapping(path = "/v1/replay", produces = MediaType.APPLICATION_JSON_VALUE)
@ConditionalOnProperty(name = "audit.replay.enabled", havingValue = "true")
public class ReplayController {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final AuditController auditController;
    
    private final AuditParameters msgHandlerAuditParams;
    
    private final AuditProperties auditProperties;
    
    private final ReplayProperties replayProperties;
    
    private final ThreadPoolTaskExecutor auditReplayExecutor;
    
    private final StatusCache statusCache;
    
    private final ApplicationContext appCtx;
    
    private final BusProperties busProperties;
    
    private final Map<String,RunningReplay> runningReplays = new HashMap<>();
    
    private final Configuration config = new Configuration();
    
    public ReplayController(AuditProperties auditProperties, ReplayProperties replayProperties, AuditController auditController,
                    @Qualifier("msgHandlerAuditParams") AuditParameters msgHandlerAuditParams, ThreadPoolTaskExecutor auditReplayExecutor,
                    StatusCache statusCache, ApplicationContext appCtx, BusProperties busProperties) {
        this.auditProperties = auditProperties;
        this.replayProperties = replayProperties;
        this.auditController = auditController;
        this.msgHandlerAuditParams = msgHandlerAuditParams;
        this.auditReplayExecutor = auditReplayExecutor;
        this.statusCache = statusCache;
        this.appCtx = appCtx;
        this.busProperties = busProperties;
        init();
    }
    
    private void init() {
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        for (String resource : auditProperties.getHdfs().getConfigResources())
            config.addResource(new Path(resource));
    }
    
    // post to create a replay
    @ApiOperation(value = "Creates an audit replay request.")
    @RequestMapping(path = "/create", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String create(@RequestParam String path, @RequestParam(defaultValue = "") String fileUri, @RequestParam(defaultValue = "100") Long sendRate,
                    @RequestParam(defaultValue = "false") boolean replayUnfinished) {
        
        log.info("Creating audit replay with params: path=" + path + ", fileUri=" + fileUri + ", sendRate=" + sendRate + ", replayUnfinished="
                        + replayUnfinished);
        
        String id = UUID.randomUUID().toString();
        
        Status status = statusCache.create(id, path, (!fileUri.isEmpty()) ? fileUri : FileSystem.getDefaultUri(config).toString(), sendRate, replayUnfinished);
        
        log.info("Created audit replay [" + status + "]");
        
        return status.getId();
    }
    
    // post to create and start a replay
    @ApiOperation(value = "Creates an audit replay request, and starts it.")
    @RequestMapping(path = "/createAndStart", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String createAndStart(@RequestParam String path, @RequestParam(defaultValue = "") String fileUri, @RequestParam(defaultValue = "100") Long sendRate,
                    @RequestParam(defaultValue = "false") boolean replayUnfinished) {
        
        log.info("Creating and starting audit replay with params: path=" + path + ", fileUri=" + fileUri + ", sendRate=" + sendRate + ", replayUnfinished="
                        + replayUnfinished);
        
        String id = UUID.randomUUID().toString();
        
        Status status = statusCache.create(id, path, (!fileUri.isEmpty()) ? fileUri : FileSystem.getDefaultUri(config).toString(), sendRate, replayUnfinished);
        runningReplays.put(id, start(status));
        
        log.info("Created and started audit replay [" + status + "]");
        
        return "Started audit replay with id " + id;
    }
    
    // post to start a replay
    @ApiOperation(value = "Starts an audit replay request.")
    @RequestMapping(path = "/{id}/start", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String start(@PathVariable String id, HttpServletResponse response) {
        
        log.info("Starting audit replay with id " + id);
        
        Status status = status(id, replayProperties.isPublishEventsEnabled());
        
        String resp;
        // if the state is 'created' or 'stopped', we can run the replay
        if (status != null) {
            if (status.getState() == CREATED) {
                RunningReplay runningReplay = start(status);
                if (runningReplay != null) {
                    runningReplays.put(id, runningReplay);
                    resp = "Started audit replay with id " + id;
                } else {
                    resp = "Cannot start audit replay with id " + id;
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                }
            } else {
                resp = "Cannot start audit replay with state " + status.getState();
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            }
        } else {
            resp = "No audit replay found with id " + id;
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
        
        log.info(resp);
        return resp;
    }
    
    private RunningReplay start(Status status) {
        status.setState(RUNNING);
        statusCache.update(status);
        
        ReplayTask replayTask = null;
        try {
            replayTask = new ReplayTask(config, status, statusCache) {
                @Override
                protected boolean audit(Map<String,String> auditParamsMap) {
                    return auditController.audit(msgHandlerAuditParams.fromMap(auditParamsMap));
                }
            };
        } catch (Exception e) {
            log.warn("Unable to create replay task for id " + status.getId(), e);
        }
        
        if (replayTask != null) {
            Future future = null;
            try {
                future = auditReplayExecutor.submit(replayTask);
            } catch (TaskRejectedException e) {
                log.warn("Unable to accept audit replay task for id " + status.getId(), e);
            }
            
            if (future != null)
                return new RunningReplay(status, future);
        }
        
        return null;
    }
    
    // post to start all replays
    @ApiOperation(value = "Starts all audit replay requests.")
    @RequestMapping(path = "/startAll", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String startAll() {
        
        log.info("Starting all audit replays");
        
        int replaysStarted = 0;
        List<Status> statuses = statusCache.retrieveAll();
        if (statuses != null) {
            for (Status status : statuses) {
                if (status.getState() == CREATED) {
                    RunningReplay replay = start(status);
                    if (replay != null) {
                        runningReplays.put(status.getId(), replay);
                        replaysStarted++;
                    }
                }
            }
        }
        
        log.info(replaysStarted + " audit replays started");
        return replaysStarted + " audit replays started";
    }
    
    // get status of a replay
    @ApiOperation(value = "Gets the status of the audit replay request.")
    @RequestMapping(path = "/{id}/status", method = org.springframework.web.bind.annotation.RequestMethod.GET)
    public Status status(@PathVariable("id") String id, HttpServletResponse response) throws IOException {
        
        log.info("Getting status for audit replay with id " + id);
        
        Status status = status(id, replayProperties.isPublishEventsEnabled());
        
        if (status == null)
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No audit replay found with id " + id);
        
        return status;
    }
    
    private Status status(String id, boolean publishEnabled) {
        Status status = statusCache.retrieve(id);
        if (status != null)
            return idleCheck(status, publishEnabled);
        return null;
    }
    
    private Status idleCheck(Status status, boolean publishEnabled) {
        // if the replay is RUNNING, and this hasn't been updated within the timeout interval, set the state to IDLE, and send out a stop request
        if (status.getState() == RUNNING && System.currentTimeMillis() - status.getLastUpdated().getTime() > replayProperties.getIdleTimeoutMillis()) {
            status.setState(IDLE);
            statusCache.update(status);
            stop(status, publishEnabled);
        }
        
        return status;
    }
    
    // get status for all replays
    @ApiOperation(value = "Lists the status for all audit replay requests.")
    @RequestMapping(path = "/statusAll", method = org.springframework.web.bind.annotation.RequestMethod.GET)
    public List<Status> statusAll() {
        
        log.info("Getting status for all audit replays");
        
        List<Status> statuses = statusCache.retrieveAll();
        if (statuses != null)
            statuses = statuses.stream().map(status -> idleCheck(status, replayProperties.isPublishEventsEnabled())).collect(Collectors.toList());
        return statuses;
    }
    
    // post to update the send rate
    @ApiOperation(value = "Updates the audit replay request.")
    @RequestMapping(path = "/{id}/update", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String update(@PathVariable("id") String id, @RequestParam Long sendRate, HttpServletResponse response) {
        
        log.info("Updating sendRate to " + sendRate + " for audit replay with id " + id);
        
        String resp;
        
        // only update if the send rate is valid
        if (sendRate >= 0) {
            // pull the replay status from cache to ensure it exists
            Status status = status(id, replayProperties.isPublishEventsEnabled());
            if (status != null) {
                update(status, sendRate, replayProperties.isPublishEventsEnabled());
                resp = "Updated audit replay with id " + id;
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp = "No audit replay found with id " + id;
            }
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp = "Send rate must be >= 0";
        }
        
        log.info(resp);
        return resp;
    }
    
    private void update(Status status, Long sendRate, boolean publishEvent) {
        // is the replay running?
        if (status.getState() == RUNNING) {
            
            // if we own it, update it. otherwise fire an event to all of the audit services
            RunningReplay replay = runningReplays.get(status.getId());
            if (replay != null) {
                replay.getStatus().setSendRate(sendRate);
                statusCache.update(replay.getStatus());
            } else if (publishEvent) {
                appCtx.publishEvent(new AuditReplayRemoteRequestEvent(this, busProperties.getId(), Request.update(status.getId(), sendRate)));
            }
        } else {
            
            // just update the cache if it's not running
            status.setSendRate(sendRate);
            statusCache.update(status);
        }
    }
    
    // post to stop a replay
    @ApiOperation(value = "Stops the audit replay request.")
    @RequestMapping(path = "/{id}/stop", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String stop(@PathVariable("id") String id, HttpServletResponse response) {
        
        log.info("Stopping audit replay with id " + id);
        
        String resp;
        
        Status status = status(id, replayProperties.isPublishEventsEnabled());
        if (status != null) {
            if (stop(status, replayProperties.isPublishEventsEnabled())) {
                resp = "Stopped audit replay with id " + id;
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp = "Cannot stop audit replay with id " + id;
            }
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp = "No audit replay found with id " + id;
        }
        
        log.info(resp);
        return resp;
    }
    
    private boolean stop(Status status, boolean publishEvent) {
        // is the replay running?
        if (status.getState() == RUNNING) {
            
            // if we own it, stop it. otherwise, fire an event to all of the audit services
            RunningReplay replay = runningReplays.get(status.getId());
            if (replay != null) {
                replay.getStatus().setState(STOPPED);
                statusCache.update(replay.getStatus());
                
                try {
                    replay.getFuture().get(replayProperties.getStopGracePeriodMillis(), TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    // interrupts and timeouts are ok. we just want to give some time to cleanup.
                }
                
                // if it's still not done, cancel it
                if (!replay.getFuture().isDone())
                    replay.getFuture().cancel(true);
                
                runningReplays.remove(status.getId());
            } else if (publishEvent) {
                appCtx.publishEvent(new AuditReplayRemoteRequestEvent(this, busProperties.getId(), Request.stop(status.getId())));
            }
        } else {
            return false;
        }
        
        return true;
    }
    
    // post to stop all replays
    @ApiOperation(value = "Stops all audit replay requests.")
    @RequestMapping(path = "/stopAll", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String stopAll() {
        
        log.info("Stopping all audit replays");
        
        int replaysStopped = stopAll(replayProperties.isPublishEventsEnabled());
        
        String resp = replaysStopped + " audit replays stopped";
        log.info(resp);
        return resp;
    }
    
    private int stopAll(boolean publishEnabled) {
        int replaysStopped = 0;
        
        if (publishEnabled)
            appCtx.publishEvent(new AuditReplayRemoteRequestEvent(this, busProperties.getId(), Request.stopAll()));
        
        // stop all of our replays. then, send an event out to all audit services to stop all replays
        List<Status> statuses = statusAll().stream().filter(status -> status.getState() == RUNNING).collect(Collectors.toList());
        for (Status status : statuses) {
            if (stop(status, false))
                replaysStopped++;
        }
        
        return replaysStopped;
    }
    
    // post to cancel a replay
    @ApiOperation(value = "Cancels the audit replay request.")
    @RequestMapping(path = "/{id}/cancel", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String cancel(@PathVariable("id") String id, HttpServletResponse response) {
        
        log.info("Canceling audit replay with id " + id);
        
        String resp;
        Status status = status(id, replayProperties.isPublishEventsEnabled());
        if (status != null) {
            if (cancel(status, replayProperties.isPublishEventsEnabled())) {
                resp = "Canceled audit replay with id " + id;
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp = "Cannot cancel audit replay with id " + id;
            }
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp = "No audit replay found with id " + id;
        }
        
        log.info(resp);
        return resp;
    }
    
    private boolean cancel(Status status, boolean publishEvent) {
        boolean success = false;
        
        // replays that are canceled, finished, or failed cannot be canceled
        if (status.getState() != CANCELED && status.getState() != FINISHED && status.getState() != FAILED) {
            
            // is the replay running?
            if (status.getState() == RUNNING) {
                
                // if we own it, stop it. otherwise, fire an event to all of the audit services
                RunningReplay replay = runningReplays.get(status.getId());
                if (replay != null) {
                    replay.getStatus().setState(CANCELED);
                    statusCache.update(replay.getStatus());
                    
                    try {
                        replay.getFuture().get(replayProperties.getStopGracePeriodMillis(), TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        // interrupts and timeouts are ok. we just want to give some time to cleanup.
                    }
                    
                    // if it's still not done, cancel it
                    if (!replay.getFuture().isDone())
                        replay.getFuture().cancel(true);
                    
                    runningReplays.remove(status.getId());
                } else if (publishEvent) {
                    appCtx.publishEvent(new AuditReplayRemoteRequestEvent(this, busProperties.getId(), Request.cancel(status.getId())));
                }
            } else {
                // just update the cache if it's not running
                status.setState(CANCELED);
                statusCache.update(status);
            }
            
            success = true;
        }
        
        return success;
    }
    
    // post to cancel all replays
    @ApiOperation(value = "Cancels all audit replay requests.")
    @RequestMapping(path = "/cancelAll", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String cancelAll() {
        
        log.info("Canceling all audit replays");
        
        int replaysCanceled = cancelAll(replayProperties.isPublishEventsEnabled());
        
        String resp = replaysCanceled + " audit replays canceled";
        log.info(resp);
        return resp;
    }
    
    private int cancelAll(boolean publishEvent) {
        int replaysCanceled = 0;
        
        if (publishEvent)
            appCtx.publishEvent(new AuditReplayRemoteRequestEvent(this, busProperties.getId(), Request.cancelAll()));
        
        // cancel all of our replays. then, send an event out to all audit services to cancel all replays
        List<Status> statuses = statusAll().stream()
                        .filter(status -> status.getState() != CANCELED && status.getState() != FINISHED && status.getState() != FAILED)
                        .collect(Collectors.toList());
        for (Status status : statuses) {
            if (cancel(status, false))
                replaysCanceled++;
        }
        
        return replaysCanceled;
    }
    
    // post to resume a replay
    @ApiOperation(value = "Resumes the audit replay request.")
    @RequestMapping(path = "/{id}/resume", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String resume(@PathVariable("id") String id, HttpServletResponse response) {
        
        log.info("Resuming audit replay with id " + id);
        
        String resp;
        Status status = status(id, replayProperties.isPublishEventsEnabled());
        if (status != null) {
            if (resume(status)) {
                resp = "Resumed audit replay with id " + id;
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp = "Cannot resume audit replay with id " + id;
            }
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp = "No audit replay found with id " + id;
        }
        
        log.info(resp);
        return resp;
    }
    
    private boolean resume(Status status) {
        // if the audit replay is idle or stopped, start it
        if (status.getState() == IDLE || status.getState() == STOPPED)
            runningReplays.put(status.getId(), start(status));
        else
            return false;
        
        return true;
    }
    
    // post to resume all replay
    @ApiOperation(value = "Resumes all audit replay requests.")
    @RequestMapping(path = "/resumeAll", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String resumeAll() {
        
        log.info("Resuming all audit replays");
        
        int replaysResumed = 0;
        
        // resume all of our replays
        List<Status> statuses = statusAll().stream().filter(status -> status.getState() == IDLE && status.getState() == STOPPED).collect(Collectors.toList());
        for (Status status : statuses) {
            runningReplays.put(status.getId(), start(status));
            replaysResumed++;
        }
        
        String resp = replaysResumed + " audit replays resumed";
        log.info(resp);
        return resp;
    }
    
    // get status of a replay
    @ApiOperation(value = "Deletes an audit replay request as long as it is not running.")
    @RequestMapping(path = "/{id}/delete", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String delete(@PathVariable("id") String id, HttpServletResponse response) {
        
        log.info("Deleting audit replay with id " + id);
        
        String resp;
        Status status = status(id, replayProperties.isPublishEventsEnabled());
        
        if (status != null) {
            if (status.getState() != RUNNING) {
                statusCache.delete(status.getId());
                resp = "Deleted audit replay with id " + id;
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp = "Cannot delete an audit replay with state " + status.getState();
            }
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp = "No audit replay found with id " + id;
        }
        
        log.info(resp);
        return resp;
    }
    
    // get status for all replays
    @ApiOperation(value = "Deletes all audit replay requests as long as they are not running.")
    @RequestMapping(path = "/deleteAll", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String deleteAll() {
        
        log.info("Deleting all audit replays");
        
        int replaysDeleted = 0;
        
        List<Status> statuses = statusAll().stream().filter(status -> status.getState() != RUNNING).collect(Collectors.toList());
        for (Status status : statuses) {
            statusCache.delete(status.getId());
            replaysDeleted++;
        }
        
        String resp = replaysDeleted + " audit replays deleted";
        log.info(resp);
        return resp;
    }
    
    public void handleRemoteRequest(Request request) {
        
        Status status = (request.getId() != null) ? status(request.getId(), replayProperties.isPublishEventsEnabled()) : null;
        
        switch (request.getMethod()) {
            case UPDATE:
                Request.UpdateRequest updateRequest = (Request.UpdateRequest) request;
                
                log.info("Updating sendRate to " + updateRequest.getSendRate() + " for audit replay with id " + updateRequest.getId());
                if (status != null) {
                    update(status, updateRequest.getSendRate(), false);
                    log.info("Updated audit replay with id " + status.getId());
                }
                break;
            
            case STOP:
                if (status != null) {
                    if (stop(status, false)) {
                        log.info("Stopped audit replay with id " + status.getId());
                    } else {
                        log.info("Cannot stop audit replay with id " + status.getId());
                    }
                }
                break;
            
            case STOP_ALL:
                int replaysStopped = stopAll(false);
                log.info(replaysStopped + " audit replays stopped");
                break;
            
            case CANCEL:
                if (status != null) {
                    if (cancel(status, false)) {
                        log.info("Stopped audit replay with id " + status.getId());
                    } else {
                        log.info("Cannot stop audit replay with id " + status.getId());
                    }
                }
                break;
            
            case CANCEL_ALL:
                int replaysCanceled = cancelAll(false);
                log.info(replaysCanceled + " audit replays canceled");
                break;
            
            default:
                log.debug("Unknown remote request method: " + request.getMethod());
        }
    }
}
