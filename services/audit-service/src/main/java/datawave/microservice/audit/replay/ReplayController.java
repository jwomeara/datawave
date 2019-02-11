package datawave.microservice.audit.replay;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.audit.AuditController;
import datawave.microservice.audit.config.AuditProperties;
import datawave.webservice.common.audit.AuditParameters;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

    private final AuditProperties auditProperties;

    private final AuditParameters restAuditParams;

    private final Configuration config = new Configuration();

    @Autowired
    public AuditController auditController;

    public ReplayController(AuditProperties auditProperties, @Qualifier("restAuditParams") AuditParameters restAuditParams) {
        this.auditProperties = auditProperties;
        this.restAuditParams = restAuditParams;
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
        String id = "";

        return id;
    }

    // get status of a replay
    @ApiOperation(value = "Gets the status of the audit replay request.")
    @RequestMapping(path = "/{id}/status", method = RequestMethod.GET)
    public ReplayStatus status(@PathVariable("id") String id) {
        ReplayStatus status = new ReplayStatus();

        return status;
    }

    // post to update the send rate
    @ApiOperation(value = "Updates the audit replay request.")
    @RequestMapping(path = "/{id}/update", method = RequestMethod.POST)
    public String update(@PathVariable("id") String id, @RequestParam Long sendRate) {
        String response = "";

        return response;
    }

    // post to cancel a replay
    @ApiOperation(value = "Stops the audit replay request.")
    @RequestMapping(path = "/{id}/stop", method = RequestMethod.POST)
    public String stop(@PathVariable("id") String id) {
        String response = "";

        return response;
    }

    // get list of replays
    @ApiOperation(value = "Lists the status for each audit replay request.")
    @RequestMapping(path = "/list", method = RequestMethod.GET)
    public List<ReplayStatus> list(@RequestParam(defaultValue = "") String state) {
        List<ReplayStatus> replays = new ArrayList<>();

        return replays;
    }

    // TODO: Break this off, make it asynchronous, and enable a 'replay status' endpoint
    // TODO: Add ability to read compressed files too

    /**
     * Reads JSON-formatted audit messages from the given path, and attempts to perform auditing on them.
     *
     * @param hdfsUri the path in hdfs where the audit files are located
     * @param path    the path in hdfs where the audit files are located
     * @return the audit IDs for the processed messages, which can be used for tracking purposes
     */
    @ApiOperation(value = "Creates an audit replay request.")
    @RequestMapping(path = "/create", method = RequestMethod.POST)
    public MultiValueMap<String, Object> replay(@RequestParam String path, @RequestParam(required = false, defaultValue = "") String hdfsUri) {
        final ObjectMapper mapper = new ObjectMapper();

        FileSystem hdfs = null;
        String selectedHdfsUri = (!hdfsUri.isEmpty()) ? hdfsUri : auditProperties.getHdfs().getHdfsUri();
        try {
            if (selectedHdfsUri != null)
                hdfs = FileSystem.get(new URI(selectedHdfsUri), config);
            else
                hdfs = FileSystem.get(config);
        } catch (Exception e) {
            log.error("Unable to determine the filesystem.", e);
        }

        List<String> auditIds = new ArrayList<>();
        long numAudits = 0;
        int filesReplayed = 0;
        int filesFailed = 0;

        if (hdfs != null) {
            // first, get a list of valid files from the directory
            List<LocatedFileStatus> replayableFiles = new ArrayList<>();

            try {
                RemoteIterator<LocatedFileStatus> filesIter = hdfs.listFiles(new Path(path), false);
                while (filesIter.hasNext()) {
                    LocatedFileStatus fileStatus = filesIter.next();
                    if (!fileStatus.getPath().getName().startsWith("_") && !fileStatus.getPath().getName().startsWith("."))
                        replayableFiles.add(fileStatus);
                }
            } catch (Exception e) {
                throw new RuntimeException("Encountered an error while listing files at [" + path + "]");
            }

            for (LocatedFileStatus replayFile : replayableFiles) {
                try {
                    // rename the file to mark it as '_REPLAYING"
                    Path replayingPath = new Path(replayFile.getPath().getParent(), "_REPLAYING." + replayFile.getPath().getName());
                    hdfs.rename(replayFile.getPath(), replayingPath);

                    // read each audit message, and process via the audit service
                    BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(replayingPath)));

                    boolean encounteredError = false;

                    TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {
                    };
                    String line = null;
                    try {
                        while (null != (line = reader.readLine())) {
                            try {
                                MultiValueMap<String, String> auditParamsMap = new LinkedMultiValueMap<>();
                                HashMap<String, String> auditParams = mapper.readValue(line, typeRef);
                                auditParams.forEach((key, value) -> auditParamsMap.add(key, urlDecodeString(value)));
                                numAudits++;

                                auditIds.add(auditController.audit(auditParamsMap));
                            } catch (Exception e) {
                                log.warn("Unable to parse a JSON audit message from [" + line + "]");
                            }
                        }
                    } catch (IOException e) {
                        encounteredError = true;
                        log.error("Unable to read line from file [" + replayingPath + "]");
                    } finally {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            encounteredError = true;
                            log.error("Unable to close file [" + replayingPath + "]");
                        }
                    }

                    Path finalPath = null;
                    if (!encounteredError) {
                        finalPath = new Path(replayFile.getPath().getParent(), "_REPLAYED." + replayFile.getPath().getName());
                        filesReplayed++;
                    } else {
                        finalPath = new Path(replayFile.getPath().getParent(), "_FAILED." + replayFile.getPath().getName());
                        filesFailed++;
                    }

                    hdfs.rename(replayingPath, finalPath);
                } catch (IOException e) {
                    log.error("Unable to replay file [" + replayFile.getPath() + "]");
                    filesFailed++;
                }
            }
        }

        MultiValueMap<String, Object> results = new LinkedMultiValueMap<>();
        results.addAll("auditIds", auditIds);
        results.add("auditsRead", numAudits);
        results.add("auditsReplayed", auditIds.size());
        results.add("filesReplayed", filesReplayed);
        results.add("filesFailed", filesFailed);

        return results;
    }

    protected List<String> urlDecodeStrings(List<String> values) {
        List<String> decoded = new ArrayList<>();
        for (String value : values)
            decoded.add(urlDecodeString(value));
        return decoded;
    }

    protected String urlDecodeString(String value) {
        try {
            return URLDecoder.decode(value, "UTF8");
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to URL encode value: " + value);
        }
        return value;
    }
}
