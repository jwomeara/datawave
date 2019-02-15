package datawave.microservice.audit.replay;

import datawave.microservice.audit.AuditController;
import datawave.microservice.audit.config.AuditProperties;
import datawave.webservice.common.audit.AuditParameters;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.AuditReplayUpdateEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.CANCELED;
import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.CREATED;
import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.FAILED;
import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.FINISHED;
import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.IDLE;
import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.RUNNING;
import static datawave.microservice.audit.replay.ReplayStatus.ReplayState.STOPPED;

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
public class ReplayController {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final AuditController auditController;

    private final AuditParameters msgHandlerAuditParams;

    private final AuditProperties auditProperties;

    private final ThreadPoolTaskExecutor auditReplayExecutor;

    private final ReplayStatusCache replayStatusCache;

    private final ApplicationContext appCtx;

    private final BusProperties busProperties;

    private final Map<String, RunningReplay> runningReplays = new HashMap<>();

    private final Configuration config = new Configuration();

    public ReplayController(AuditController auditController, @Qualifier("msgHandlerAuditParams") AuditParameters msgHandlerAuditParams, AuditProperties auditProperties, ThreadPoolTaskExecutor auditReplayExecutor, ReplayStatusCache replayStatusCache, ApplicationContext appCtx, BusProperties busProperties) {
        this.auditController = auditController;
        this.msgHandlerAuditParams = msgHandlerAuditParams;
        this.auditProperties = auditProperties;
        this.auditReplayExecutor = auditReplayExecutor;
        this.replayStatusCache = replayStatusCache;
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
    @RequestMapping(path = "/create", method = RequestMethod.POST)
    public String create(@RequestParam String path, @RequestParam(defaultValue = "") String hdfsUri, @RequestParam(defaultValue = "100") Long sendRate) {
        String id = UUID.randomUUID().toString();

        ReplayStatus status = replayStatusCache.create(id, path, hdfsUri, sendRate);

        return status.getId();
    }

    // post to create and start a replay
    @ApiOperation(value = "Creates an audit replay request, and starts it.")
    @RequestMapping(path = "/createAndStart", method = RequestMethod.POST)
    public String createAndStart(@RequestParam String path, @RequestParam(defaultValue = "") String hdfsUri, @RequestParam(defaultValue = "100") Long sendRate) {
        String id = UUID.randomUUID().toString();

        runningReplays.put(id, start(replayStatusCache.create(id, path, hdfsUri, sendRate)));

        return "Started audit replay with id " + id;    }

    // post to start a replay
    @ApiOperation(value = "Starts an audit replay request.")
    @RequestMapping(path = "/{id}/start", method = RequestMethod.POST)
    public String start(@RequestParam String id) {
        ReplayStatus status = status(id);

        // if the state is 'created' or 'stopped', we can run the replay
        if (status != null) {
            if (status.getState() == CREATED) {
                runningReplays.put(id, start(status));
            } else {
                throw new RuntimeException("Cannot start audit replay with state " + status.getState());
            }
        } else {
            throw new RuntimeException("No audit replay found with id " + id);
        }

        return "Started audit replay with id " + id;
    }

    private RunningReplay start(ReplayStatus status) {
        status.setState(RUNNING);
        replayStatusCache.update(status);

        RunningReplay replay = new RunningReplay(status);
        ReplayTask replayTask = null;
        try {
            replayTask = new ReplayTask(config, status, replayStatusCache) {
                @Override
                protected boolean audit(Map<String, String> auditParamsMap) {
                    return auditController.audit(msgHandlerAuditParams.fromMap(auditParamsMap));
                }
            };
        } catch (Exception e) {
            throw new RuntimeException("Unable to create replay task");
        }

        replay.setFuture(auditReplayExecutor.submit(replayTask));

        return replay;
    }

    // post to start all replays
    @ApiOperation(value = "Starts all audit replay requests.")
    @RequestMapping(path = "/startAll", method = RequestMethod.POST)
    public String startAll() {
        int replaysStarted = 0;
        List<ReplayStatus> replayStatuses = replayStatusCache.retrieveAll();
        if (replayStatuses != null) {
            for (ReplayStatus status : replayStatuses) {
                if (status.getState() == CREATED) {
                    RunningReplay replay = start(status);
                    runningReplays.put(status.getId(), replay);
                    replaysStarted++;
                }
            }
        }

        return replaysStarted + " audit replays started";    }

    // get status of a replay
    @ApiOperation(value = "Gets the status of the audit replay request.")
    @RequestMapping(path = "/{id}/status", method = RequestMethod.GET)
    public ReplayStatus status(@PathVariable("id") String id) {
        ReplayStatus status = replayStatusCache.retrieve(id);
        if (status != null)
            return idleCheck(status);
        else
            throw new RuntimeException("No audit replay found with id " + id);
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

    // get it from the cache every time so that the response is consistent across audit service pods
    public List<ReplayStatus> statusAll() {
        List<ReplayStatus> replayStatuses = replayStatusCache.retrieveAll();
        if (replayStatuses != null)
            replayStatuses = replayStatuses.stream().map(this::idleCheck).collect(Collectors.toList());

        return replayStatuses;
    }

    // get status for all replays
    @ApiOperation(value = "Lists the status for all audit replay requests.")
    @RequestMapping(path = "/statusAll", method = RequestMethod.GET)
    public List<ReplayStatus> statusAll(@RequestParam(defaultValue = "") String state) {
        List<ReplayStatus> replayStatuses = replayStatusCache.retrieveAll();
        if (replayStatuses != null)
            replayStatuses = replayStatuses.stream().map(this::idleCheck).collect(Collectors.toList());
        return replayStatuses;
    }

    // post to update the send rate
    @ApiOperation(value = "Updates the audit replay request.")
    @RequestMapping(path = "/{id}/update", method = RequestMethod.POST)
    public String update(@PathVariable("id") String id, @RequestParam Long sendRate) {
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

        return "Updated audit replay with id " + id;    }

    // post to stop a replay
    @ApiOperation(value = "Stops the audit replay request.")
    @RequestMapping(path = "/{id}/stop", method = RequestMethod.POST)
    public String stop(@PathVariable("id") String id) {
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
        // is the replay running?
        if (status.getState() == RUNNING) {

            // if we own it, stop it.  otherwise, fire an event to all of the audit services
            RunningReplay replay = runningReplays.get(status.getId());
            if (replay != null) {

                replay.getStatus().setState(STOPPED);
                replayStatusCache.update(replay.getStatus());

                try {
                    replay.getFuture().get(500L, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    // interrupts and timeouts are ok.  we just want to give some time to cleanup.
                }

                // if it's still not done, cancel it
                if (!replay.getFuture().isDone())
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
    @ApiOperation(value = "Stops all audit replay requests.")
    @RequestMapping(path = "/stopAll", method = RequestMethod.POST)
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
    @ApiOperation(value = "Cancels the audit replay request.")
    @RequestMapping(path = "/{id}/cancel", method = RequestMethod.POST)
    public String cancel(@PathVariable("id") String id) {
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
        // replays that are canceled, finished, or failed cannot be canceled
        if (status.getState() != CANCELED && status.getState() != FINISHED && status.getState() != FAILED) {

            // is the replay running?
            if (status.getState() == RUNNING) {

                // if we own it, stop it.  otherwise, fire an event to all of the audit services
                RunningReplay replay = runningReplays.get(status.getId());
                if (replay != null) {

                    replay.getStatus().setState(CANCELED);
                    replayStatusCache.update(replay.getStatus());

                    try {
                        replay.getFuture().get(500L, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        // interrupts and timeouts are ok.  we just want to give some time to cleanup.
                    }

                    // if it's still not done, cancel it
                    if (!replay.getFuture().isDone())
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
    @ApiOperation(value = "Cancels all audit replay requests.")
    @RequestMapping(path = "/cancelAll", method = RequestMethod.POST)
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
    @ApiOperation(value = "Resumes the audit replay request.")
    @RequestMapping(path = "/{id}/resume", method = RequestMethod.POST)
    public String resume(@PathVariable("id") String id) {
        ReplayStatus status = status(id);
        if (status != null) {
            if (!resume(status))
                throw new RuntimeException("Cannot resume audit replay with id " + id);
        } else {
            throw new RuntimeException("No audit replay found with id " + id);
        }

        return "Resumed audit replay with id " + id;
    }

    private boolean resume(ReplayStatus status) {
        // if the audit replay is idle or stopped, start it
        if (status.getState() == IDLE || status.getState() == STOPPED) {

            runningReplays.put(status.getId(), start(status));
        } else {

            return false;
        }

        return true;
    }

    // post to resume all replay
    @ApiOperation(value = "Resumes all audit replay requests.")
    @RequestMapping(path = "/resumeAll", method = RequestMethod.POST)
    public String resumeAll() {
        int replaysResumed = 0;

        // resume all of our replays
        List<ReplayStatus> replayStatuses = statusAll().stream().filter(status -> status.getState() == IDLE && status.getState() == STOPPED).collect(Collectors.toList());
        for (ReplayStatus status : replayStatuses) {
            runningReplays.put(status.getId(), start(status));
            replaysResumed++;
        }

        return replaysResumed + " audit replays resumed";
    }

    // TODO: Move this logic to the ReplayTask
    // TODO: Add ability to read compressed files too

//    /**
//     * Reads JSON-formatted audit messages from the given path, and attempts to perform auditing on them.
//     *
//     * @param hdfsUri the path in hdfs where the audit files are located
//     * @param path    the path in hdfs where the audit files are located
//     * @return the audit IDs for the processed messages, which can be used for tracking purposes
//     */
//    @ApiOperation(value = "Creates an audit replay request.")
//    @RequestMapping(path = "/create", method = RequestMethod.POST)
//    public MultiValueMap<String, Object> create(@RequestParam String path, @RequestParam(required = false, defaultValue = "") String hdfsUri) {
//        final ObjectMapper mapper = new ObjectMapper();
//
//        FileSystem hdfs = null;
//        String selectedHdfsUri = (!hdfsUri.isEmpty()) ? hdfsUri : auditProperties.getHdfs().getHdfsUri();
//        try {
//            if (selectedHdfsUri != null)
//                hdfs = FileSystem.get(new URI(selectedHdfsUri), config);
//            else
//                hdfs = FileSystem.get(config);
//        } catch (Exception e) {
//            log.error("Unable to determine the filesystem.", e);
//        }
//
//        List<String> auditIds = new ArrayList<>();
//        long numAudits = 0;
//        int filesReplayed = 0;
//        int filesFailed = 0;
//
//        if (hdfs != null) {
//            // first, get a list of valid files from the directory
//            List<LocatedFileStatus> replayableFiles = new ArrayList<>();
//
//            try {
//                RemoteIterator<LocatedFileStatus> filesIter = hdfs.listFiles(new Path(path), false);
//                while (filesIter.hasNext()) {
//                    LocatedFileStatus fileStatus = filesIter.next();
//                    if (!fileStatus.getPath().getName().startsWith("_") && !fileStatus.getPath().getName().startsWith("."))
//                        replayableFiles.add(fileStatus);
//                }
//            } catch (Exception e) {
//                throw new RuntimeException("Encountered an error while listing files at [" + path + "]");
//            }
//
//            for (LocatedFileStatus replayFile : replayableFiles) {
//                try {
//                    // rename the file to mark it as '_REPLAYING"
//                    Path replayingPath = new Path(replayFile.getPath().getParent(), "_REPLAYING." + replayFile.getPath().getName());
//                    hdfs.rename(replayFile.getPath(), replayingPath);
//
//                    // read each audit message, and process via the audit service
//                    BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(replayingPath)));
//
//                    boolean encounteredError = false;
//
//                    TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {
//                    };
//                    String line = null;
//                    try {
//                        while (null != (line = reader.readLine())) {
//                            try {
//                                MultiValueMap<String, String> auditParamsMap = new LinkedMultiValueMap<>();
//                                HashMap<String, String> auditParams = mapper.readValue(line, typeRef);
//                                auditParams.forEach((key, value) -> auditParamsMap.add(key, urlDecodeString(value)));
//                                numAudits++;
//
//                                auditIds.add(auditController.audit(auditParamsMap));
//                            } catch (Exception e) {
//                                log.warn("Unable to parse a JSON audit message from [" + line + "]");
//                            }
//                        }
//                    } catch (IOException e) {
//                        encounteredError = true;
//                        log.error("Unable to read line from file [" + replayingPath + "]");
//                    } finally {
//                        try {
//                            reader.close();
//                        } catch (IOException e) {
//                            encounteredError = true;
//                            log.error("Unable to close file [" + replayingPath + "]");
//                        }
//                    }
//
//                    Path finalPath = null;
//                    if (!encounteredError) {
//                        finalPath = new Path(replayFile.getPath().getParent(), "_REPLAYED." + replayFile.getPath().getName());
//                        filesReplayed++;
//                    } else {
//                        finalPath = new Path(replayFile.getPath().getParent(), "_FAILED." + replayFile.getPath().getName());
//                        filesFailed++;
//                    }
//
//                    hdfs.rename(replayingPath, finalPath);
//                } catch (IOException e) {
//                    log.error("Unable to replay file [" + replayFile.getPath() + "]");
//                    filesFailed++;
//                }
//            }
//        }
//
//        MultiValueMap<String, Object> results = new LinkedMultiValueMap<>();
//        results.addAll("auditIds", auditIds);
//        results.add("auditsRead", numAudits);
//        results.add("auditsReplayed", auditIds.size());
//        results.add("filesReplayed", filesReplayed);
//        results.add("filesFailed", filesFailed);
//
//        return results;
//    }
//
//    protected List<String> urlDecodeStrings(List<String> values) {
//        List<String> decoded = new ArrayList<>();
//        for (String value : values)
//            decoded.add(urlDecodeString(value));
//        return decoded;
//    }
//
//    protected String urlDecodeString(String value) {
//        try {
//            return URLDecoder.decode(value, "UTF8");
//        } catch (UnsupportedEncodingException e) {
//            log.error("Unable to URL encode value: " + value);
//        }
//        return value;
//    }
}
