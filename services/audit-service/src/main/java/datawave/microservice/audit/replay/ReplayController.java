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
import io.swagger.annotations.ApiParam;
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

import static datawave.microservice.audit.replay.status.Status.ReplayState.CREATED;
import static datawave.microservice.audit.replay.status.Status.ReplayState.IDLE;
import static datawave.microservice.audit.replay.status.Status.ReplayState.RUNNING;
import static datawave.microservice.audit.replay.status.Status.ReplayState.STOPPED;

/**
 * The ReplayController presents the REST endpoints for audit replay.
 *
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
        if (auditProperties.getFs().getConfigResources() != null) {
            for (String resource : auditProperties.getFs().getConfigResources())
                config.addResource(new Path(resource));
        }
    }

    /**
     * Creates an audit replay request
     *
     * @param path
     *            The path where the audit file(s) to be replayed can be found
     * @param fileUri
     *            The file URI to use, if the default is not desired
     * @param sendRate
     *            The number of messages to send per second
     * @param replayUnfinishedFiles
     *            Indicates whether files from an unfinished audit replay should be included
     * @return the audit replay id
     */
    @ApiOperation(value = "Creates an audit replay request.")
    @RequestMapping(path = "/create", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String create(@ApiParam(value = "The path where the audit file(s) to be replayed can be found", required = true) @RequestParam String path,
                         @ApiParam("The file URI to use, relative to the path") @RequestParam(defaultValue = "") String fileUri,
                         @ApiParam(value = "The number of messages to send per second", defaultValue = "100") @RequestParam(defaultValue = "100") Long sendRate,
                         @ApiParam(value = "Indicates whether files from an unfinished audit replay should be included", defaultValue = "false") @RequestParam(defaultValue = "false") boolean replayUnfinishedFiles) {
        
        log.info("Creating audit replay with params: path=" + path + ", fileUri=" + fileUri + ", sendRate=" + sendRate + ", replayUnfinishedFiles="
                        + replayUnfinishedFiles);
        
        String id = UUID.randomUUID().toString();
        
        Status status = statusCache.create(id, path, (!fileUri.isEmpty()) ? fileUri : FileSystem.getDefaultUri(config).toString(), sendRate, replayUnfinishedFiles);
        
        log.info("Created audit replay [" + status + "]");
        
        return status.getId();
    }

    /**
     * Creates an audit replay request, and starts it
     *
     * @param path
     *            The path where the audit file(s) to be replayed can be found
     * @param fileUri
     *            The file URI to use, if the default is not desired
     * @param sendRate
     *            The number of messages to send per second
     * @param replayUnfinishedFiles
     *            Indicates whether files from an unfinished audit replay should be included
     * @return the audit replay id
     */
    @ApiOperation(value = "Creates an audit replay request, and starts it.")
    @RequestMapping(path = "/createAndStart", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String createAndStart(@ApiParam(value = "The path where the audit file(s) to be replayed can be found", required = true) @RequestParam String path,
                                 @ApiParam("The file URI to use, relative to the path") @RequestParam(defaultValue = "") String fileUri,
                                 @ApiParam(value = "The number of messages to send per second", defaultValue = "100") @RequestParam(defaultValue = "100") Long sendRate,
                                 @ApiParam(value = "Indicates whether files from an unfinished audit replay should be included", defaultValue = "false") @RequestParam(defaultValue = "false") boolean replayUnfinishedFiles) {
        
        log.info("Creating and starting audit replay with params: path=" + path + ", fileUri=" + fileUri + ", sendRate=" + sendRate + ", replayUnfinishedFiles="
                        + replayUnfinishedFiles);
        
        String id = UUID.randomUUID().toString();
        
        Status status = statusCache.create(id, path, (!fileUri.isEmpty()) ? fileUri : FileSystem.getDefaultUri(config).toString(), sendRate, replayUnfinishedFiles);
        runningReplays.put(id, start(status));
        
        log.info("Created and started audit replay [" + status + "]");
        
        return id;
    }


    /**
     * Starts an audit replay
     *
     * @param id
     *            The audit replay id
     * @return status, indicating whether the audit replay was started successfully
     */
    @ApiOperation(value = "Starts an audit replay.")
    @RequestMapping(path = "/{id}/start", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String start(@ApiParam(value = "The audit replay id") @PathVariable String id, HttpServletResponse response) {
        
        log.info("Starting audit replay with id " + id);
        
        Status status = status(id, replayProperties.isPublishEvents());
        
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

    /**
     * Starts all audit replays
     *
     * @return status, indicating the number of audit replays which were successfully started
     */
    @ApiOperation(value = "Starts all audit replays.")
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

    /**
     * Gets the status of an audit replay
     *
     * @param id
     *            The audit replay id
     * @return the status of the audit replay
     */
    @ApiOperation(value = "Gets the status of an audit replay.")
    @RequestMapping(path = "/{id}/status", method = org.springframework.web.bind.annotation.RequestMethod.GET)
    public Status status(@ApiParam("The audit replay id") @PathVariable("id") String id, HttpServletResponse response) throws IOException {
        
        log.info("Getting status for audit replay with id " + id);
        
        Status status = status(id, replayProperties.isPublishEvents());
        
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

    /**
     * Lists the status for all audit replays
     *
     * @return list of statuses for all audit replays
     */
    @ApiOperation(value = "Lists the status for all audit replays.")
    @RequestMapping(path = "/statusAll", method = org.springframework.web.bind.annotation.RequestMethod.GET)
    public List<Status> statusAll() {
        
        log.info("Getting status for all audit replays");
        
        List<Status> statuses = statusCache.retrieveAll();
        if (statuses != null)
            statuses = statuses.stream().map(status -> idleCheck(status, replayProperties.isPublishEvents())).collect(Collectors.toList());
        return statuses;
    }

    /**
     * Updates an audit replay
     *
     * @param id
     *            The audit replay id
     * @param sendRate
     *            The number of messages to send per second
     * @return status, indicating whether the update was successful
     */
    @ApiOperation(value = "Updates an audit replay.")
    @RequestMapping(path = "/{id}/update", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String update(@ApiParam("The audit replay id") @PathVariable("id") String id,
                         @ApiParam(value = "The number of messages to send per second", required = true) @RequestParam long sendRate, HttpServletResponse response) {
        
        log.info("Updating sendRate to " + sendRate + " for audit replay with id " + id);
        
        String resp;
        
        // only update if the send rate is valid
        if (sendRate >= 0) {
            // pull the replay status from cache to ensure it exists
            Status status = status(id, replayProperties.isPublishEvents());
            if (status != null) {
                update(status, sendRate, replayProperties.isPublishEvents());
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
    
    private void update(Status status, long sendRate, boolean publishEvent) {
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
            
            // just update the cache if it'Filesystem not running
            status.setSendRate(sendRate);
            statusCache.update(status);
        }
    }

    /**
     * Updates all audit replays
     *
     * @param sendRate
     *            The number of messages to send per second
     * @return status, indicating the number of audit replays which were successfully updated
     */
    @ApiOperation(value = "Updates all audit replays.")
    @RequestMapping(path = "/updateAll", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String updateAll(@ApiParam(value = "The number of messages to send per second", required = true) @RequestParam long sendRate, HttpServletResponse response) {

        log.info("Updating sendRate to " + sendRate + " for all audit replays");

        String resp;

        // only update if the send rate is valid
        if (sendRate >= 0) {
            resp = updateAll(sendRate, replayProperties.isPublishEvents(), false) + "audit replays updated";
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp = "Send rate must be >= 0";
        }

        log.info(resp);
        return resp;
    }

    private int updateAll(long sendRate, boolean publishEnabled, boolean onlyUpdateRunning) {
        int replaysUpdated = 0;

        if (publishEnabled)
            appCtx.publishEvent(new AuditReplayRemoteRequestEvent(this, busProperties.getId(), Request.updateAll(sendRate)));

        // update all of our replays.
        List<Status> statuses = statusAll();
        if (onlyUpdateRunning)
            statuses = statuses.stream().filter(status -> status.getState() == RUNNING).collect(Collectors.toList());

        for (Status status : statuses) {
            update(status, sendRate, false);
            replaysUpdated++;
        }

        return replaysUpdated;
    }

    /**
     * Stops an audit replay
     *
     * @param id
     *            The audit replay id
     * @return status, indicating whether the audit replay was successfully stopped
     */
    @ApiOperation(value = "Stops an audit replay.")
    @RequestMapping(path = "/{id}/stop", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String stop(@ApiParam("The audit replay id") @PathVariable("id") String id, HttpServletResponse response) {
        
        log.info("Stopping audit replay with id " + id);
        
        String resp;
        
        Status status = status(id, replayProperties.isPublishEvents());
        if (status != null) {
            if (stop(status, replayProperties.isPublishEvents())) {
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

    /**
     * Stops all audit replays
     *
     * @return status, indicating the number of audit replays which were successfully stopped
     */
    @ApiOperation(value = "Stops all audit replays.")
    @RequestMapping(path = "/stopAll", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String stopAll() {
        
        log.info("Stopping all audit replays");
        
        int replaysStopped = stopAll(replayProperties.isPublishEvents());
        
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

    /**
     * Resumes an audit replay
     *
     * @param id
     *            The audit replay id
     * @return status, indicating whether the audit replay was successfully resumed
     */
    @ApiOperation(value = "Resumes an audit replay.")
    @RequestMapping(path = "/{id}/resume", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String resume(@ApiParam("The audit replay id") @PathVariable("id") String id, HttpServletResponse response) {
        
        log.info("Resuming audit replay with id " + id);
        
        String resp;
        Status status = status(id, replayProperties.isPublishEvents());
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

    /**
     * Resumes all audit replays
     *
     * @return status, indicating the number of audit replays which were successfully resumed
     */
    @ApiOperation(value = "Resumes all audit replays.")
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

    /**
     * Deletes an audit replay
     *
     * @param id
     *            The audit replay id
     * @return status, indicating whether the audit replay was successfully deleted
     */
    @ApiOperation(value = "Deletes an audit replay.")
    @RequestMapping(path = "/{id}/delete", method = org.springframework.web.bind.annotation.RequestMethod.POST)
    public String delete(@ApiParam("The audit replay id") @PathVariable("id") String id, HttpServletResponse response) {
        
        log.info("Deleting audit replay with id " + id);
        
        String resp;
        Status status = status(id, replayProperties.isPublishEvents());
        
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

    /**
     * Deletes all audit replays
     *
     * @return status, indicating the number of audit replays which were successfully deleted
     */
    @ApiOperation(value = "Deletes all audit replays.")
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
        
        Status status = (request.getId() != null) ? status(request.getId(), replayProperties.isPublishEvents()) : null;

        Request.UpdateRequest updateRequest = (request instanceof Request.UpdateRequest) ? (Request.UpdateRequest) request : null;

        switch (request.getMethod()) {
            case UPDATE:
                if (updateRequest != null) {
                    log.info("Updating sendRate to " + updateRequest.getSendRate() + " for audit replay with id " + updateRequest.getId());
                    if (status != null) {
                        update(status, updateRequest.getSendRate(), false);
                        log.info("Updated audit replay with id " + status.getId());
                    }
                }
                break;

            case UPDATE_ALL:
                if (updateRequest != null) {
                    int replaysUpdated = updateAll(updateRequest.getSendRate(), false, true);
                    log.info(replaysUpdated + " audit replays updated");
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
            
            default:
                log.debug("Unknown remote request method: " + request.getMethod());
        }
    }
}
