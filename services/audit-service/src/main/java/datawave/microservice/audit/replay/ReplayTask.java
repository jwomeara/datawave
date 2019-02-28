package datawave.microservice.audit.replay;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static datawave.microservice.audit.replay.ReplayStatus.ReplayState;
import static datawave.microservice.audit.replay.ReplayStatus.FileState;

public abstract class ReplayTask implements Runnable {
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final Configuration config;
    private final ReplayStatus status;
    private final ReplayStatusCache replayStatusCache;
    
    private FileSystem hdfs;
    
    public ReplayTask(Configuration config, ReplayStatus status, ReplayStatusCache replayStatusCache) throws Exception {
        this.config = config;
        this.status = status;
        this.replayStatusCache = replayStatusCache;
        this.hdfs = FileSystem.get(new URI(status.getHdfsUri()), config);
    }
    
    @Override
    public void run() {
        
        if (status.getState() != ReplayState.RUNNING)
            return;
        
        // if we need to, get a list of files
        if (status.getFiles().isEmpty())
            status.setFiles(listFiles());
        
        // sort the files to process. 'RUNNING' first, followed by 'QUEUED'
        List<ReplayStatus.FileStatus> filesToProcess = status.getFiles().stream()
                        .filter(fileStatus -> fileStatus.getState() == FileState.RUNNING || fileStatus.getState() == FileState.QUEUED)
                        .sorted((o1, o2) -> o2.getState().ordinal() - o1.getState().ordinal()).collect(Collectors.toList());
        
        // then, process any unmarked/remaining files
        for (ReplayStatus.FileStatus fileStatus : filesToProcess) {
            if (!processFile(fileStatus)) {
                status.setState(ReplayState.FAILED);
                break;
            } else if (status.getState() != ReplayState.RUNNING) {
                break;
            }
            
            replayStatusCache.update(status);
        }
        
        // if we're still running, finish the replay
        if (status.getState() == ReplayState.RUNNING) {
            // finally, update our status as FINISHED
            if (status.getState() != ReplayState.FAILED)
                status.setState(ReplayState.FINISHED);
        }
        
        replayStatusCache.update(status);
    }
    
    private List<ReplayStatus.FileStatus> listFiles() {
        List<ReplayStatus.FileStatus> fileStatuses = new ArrayList<>();
        
        try {
            RemoteIterator<LocatedFileStatus> filesIter = hdfs.listFiles(new Path(status.getPath()), false);
            while (filesIter.hasNext()) {
                LocatedFileStatus locatedFile = filesIter.next();
                String fileName = locatedFile.getPath().getName();
                
                if (fileName.startsWith("_" + FileState.RUNNING)) {
                    fileStatuses.add(new ReplayStatus.FileStatus(locatedFile.getPath().toString(), FileState.RUNNING));
                } else if (fileName.startsWith("_" + FileState.FINISHED)) {
                    fileStatuses.add(new ReplayStatus.FileStatus(locatedFile.getPath().toString(), FileState.FINISHED));
                } else if (fileName.startsWith("_" + FileState.FAILED)) {
                    fileStatuses.add(new ReplayStatus.FileStatus(locatedFile.getPath().toString(), FileState.FAILED));
                } else if (!locatedFile.getPath().getName().startsWith("_") && !locatedFile.getPath().getName().startsWith(".")) {
                    Path queuedFile = renameFile(FileState.QUEUED, locatedFile.getPath());
                    if (queuedFile != null) {
                        fileStatuses.add(new ReplayStatus.FileStatus(queuedFile.toString(), FileState.QUEUED));
                    } else {
                        log.warn("Unable to queue file \"" + locatedFile.getPath() + "\"");
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Encountered an error while listing files at [" + status.getPath() + "]");
        }
        
        return fileStatuses;
    }
    
    private boolean processFile(ReplayStatus.FileStatus fileStatus) {
        Path file = new Path(fileStatus.getPath());
        
        long numToSkip = 0;
        if (fileStatus.getState() == FileState.RUNNING) {
            
            numToSkip = fileStatus.getLinesRead();
        } else {
            
            Path runningFile = renameFile(FileState.RUNNING, file);
            if (runningFile != null) {
                file = runningFile;
            } else {
                log.error("Unable to rename file from \"" + file + "\" to \"" + runningFile + "\"");
                fileStatus.setState(FileState.FAILED);
                return false;
            }
        }
        
        fileStatus.setPath(file.toString());
        fileStatus.setState(FileState.RUNNING);
        replayStatusCache.update(status);
        
        boolean encounteredError = false;
        long linesRead = fileStatus.getLinesRead();
        long auditsSent = fileStatus.getAuditsSent();
        long auditsFailed = fileStatus.getAuditsFailed();
        
        BufferedReader reader = null;
        try {
            // read each audit message, and process via the audit service
            reader = new BufferedReader(new InputStreamReader(hdfs.open(file)));
            TypeReference<HashMap<String,String>> typeRef = new TypeReference<HashMap<String,String>>() {};
            
            String line = null;
            while (null != (line = reader.readLine()) && status.getState() == ReplayState.RUNNING) {
                if (++linesRead > numToSkip && status.getState() == ReplayState.RUNNING) {
                    try {
                        // send rate of 0 will pause the audit replay
                        long sendRate = status.getSendRate();
                        while (sendRate == 0) {
                            try {
                                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                            } catch (InterruptedException e) {
                                // not a problem if we exit a little early
                            }
                            sendRate = status.getSendRate();
                        }
                        
                        HashMap<String,String> auditParamsMap = mapper.readValue(line, typeRef);
                        auditParamsMap.forEach((key, value) -> auditParamsMap.put(key, urlDecodeString(value)));
                        
                        if (!audit(auditParamsMap)) {
                            log.warn("Failed to audit: " + auditParamsMap);
                            auditsFailed++;
                        }
                        auditsSent++;
                        
                        try {
                            Thread.sleep((long) (1000.0 / sendRate));
                        } catch (InterruptedException e) {
                            // not a problem if we exit a little early
                        }
                    } catch (IOException e) {
                        log.warn("Unable to parse a JSON audit message from [" + line + "]");
                    }
                }
                
                // occasionally update the cached status
                if (linesRead % 1000 == 0) {
                    fileStatus.setLinesRead(linesRead);
                    fileStatus.setAuditsSent(auditsSent);
                    fileStatus.setAuditsFailed(auditsFailed);
                    replayStatusCache.update(status);
                }
            }
        } catch (IOException e) {
            encounteredError = true;
            log.error("Unable to read from file [" + file + "]");
        }
        
        fileStatus.setLinesRead(linesRead);
        fileStatus.setAuditsSent(auditsSent);
        fileStatus.setAuditsFailed(auditsFailed);
        
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                log.error("Unable to close file [" + file + "]");
            }
        }
        
        if (status.getState() == ReplayState.RUNNING) {
            
            FileState fileState = (encounteredError) ? FileState.FAILED : FileState.FINISHED;
            Path finalPath = renameFile(fileState, file);

            if (finalPath != null) {
                fileStatus.setState(fileState);
                fileStatus.setPath(finalPath.toString());
            } else {
                fileStatus.setState(FileState.FAILED);
                log.error("Unable to rename file \"" + file + "\" to \"" + finalPath + "\"");
                return false;
            }
        }
        
        return true;
    }
    
    private Path renameFile(FileState newState, Path file) {
        String prefix = "_" + newState + ".";
        String fileName = file.getName();
        fileName = (fileName.startsWith("_")) ? prefix + fileName.substring(fileName.indexOf('.') + 1) : prefix + fileName;
        Path renamedFile = new Path(file.getParent(), fileName);
        try {
            if (hdfs.rename(file, renamedFile))
                return renamedFile;
        } catch (IOException e) {
            log.warn("Unable to rename file from \"" + file + "\" to \"" + renamedFile + "\"");
        }
        return null;
    }
    
    // TODO: Add ability to do a timed safe stop/cancel
    
    protected String urlDecodeString(String value) {
        try {
            return URLDecoder.decode(value, "UTF8");
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to decode URL value: " + value);
        }
        return value;
    }
    
    abstract protected boolean audit(Map<String,String> auditParamsMap);
}
