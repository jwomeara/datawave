package datawave.microservice.audit.replay;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.webservice.common.audit.AuditParameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ReplayTask implements Runnable {

    private static final ObjectMapper mapper = new ObjectMapper();

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Configuration config;
    private final ReplayStatus status;

    private FileSystem hdfs;

    public ReplayTask(Configuration config, ReplayStatus status) throws Exception {
        this.config = config;
        this.status = status;
        init();
    }

    private void init() throws Exception {
        if (status.getHdfsUri() != null)
            hdfs = FileSystem.get(new URI(status.getHdfsUri()), config);
        else
            hdfs = FileSystem.get(config);
    }

    @Override
    public void run() {
        long i = 0;
        while (i++ >= 0) {
            // do nothing
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // TODO: Add ability to do a timed safe stop/cancel

    // TODO: Rethink how messages should be passed to the audit controller

    public MultiValueMap<String, Object> replay() {

        List<String> auditIds = new ArrayList<>();
        long numAudits = 0;
        int filesReplayed = 0;
        int filesFailed = 0;

        if (hdfs != null) {
            // first, get a list of valid files from the directory
            List<LocatedFileStatus> replayableFiles = new ArrayList<>();

            try {
                RemoteIterator<LocatedFileStatus> filesIter = hdfs.listFiles(new Path(status.getPath()), false);
                while (filesIter.hasNext()) {
                    LocatedFileStatus fileStatus = filesIter.next();
                    if (!fileStatus.getPath().getName().startsWith("_") && !fileStatus.getPath().getName().startsWith("."))
                        replayableFiles.add(fileStatus);
                }
            } catch (Exception e) {
                throw new RuntimeException("Encountered an error while listing files at [" + status.getPath() + "]");
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
                                HashMap<String, String> auditParamsMap = mapper.readValue(line, typeRef);
                                AuditParameters auditParams = new AuditParameters().fromMap(auditParamsMap);
                                numAudits++;

                                auditIds.add(auditController.audit(auditParams));
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
