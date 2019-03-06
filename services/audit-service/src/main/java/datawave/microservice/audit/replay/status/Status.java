package datawave.microservice.audit.replay.status;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class Status {
    
    public enum ReplayState {
        CREATED, RUNNING, IDLE, STOPPED, CANCELED, FINISHED, FAILED
    }
    
    public enum FileState {
        QUEUED, RUNNING, FINISHED, FAILED
    }
    
    private String id;
    private ReplayState state;
    private String fileUri;
    private String path;
    private long sendRate;
    private List<FileStatus> files = new ArrayList<>();
    private Date lastUpdated;
    private boolean replayUnfinished;
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public ReplayState getState() {
        return state;
    }
    
    public void setState(ReplayState state) {
        this.state = state;
    }
    
    public String getFileUri() {
        return fileUri;
    }
    
    public void setFileUri(String fileUri) {
        this.fileUri = fileUri;
    }
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public long getSendRate() {
        return sendRate;
    }
    
    public void setSendRate(long sendRate) {
        this.sendRate = sendRate;
    }
    
    public List<FileStatus> getFiles() {
        return files;
    }
    
    public void setFiles(List<FileStatus> files) {
        this.files = files;
    }
    
    public Date getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public boolean isReplayUnfinished() {
        return replayUnfinished;
    }
    
    public void setReplayUnfinished(boolean replayUnfinished) {
        this.replayUnfinished = replayUnfinished;
    }
    
    @Override
    public String toString() {
        return "{id:" + id + ", state:" + state + ", fileUri:" + fileUri + ", path:" + path + ", sendRate:" + sendRate + ", files:["
                        + String.join(",", files.stream().map(FileStatus::toString).collect(Collectors.toSet())) + "], lastUpdated:" + lastUpdated + "}";
    }
    
    public static final class FileStatus {
        private FileState state;
        private String path;
        private long linesRead;
        private long auditsSent;
        private long auditsFailed;
        private boolean encounteredError;
        
        public FileStatus(String path, FileState state) {
            this.path = path;
            this.state = state;
            this.linesRead = 0;
            this.auditsSent = 0;
            this.auditsFailed = 0;
            this.encounteredError = false;
        }
        
        public FileState getState() {
            return state;
        }
        
        public void setState(FileState state) {
            this.state = state;
        }
        
        public String getPath() {
            return path;
        }
        
        public void setPath(String path) {
            this.path = path;
        }
        
        public long getLinesRead() {
            return linesRead;
        }
        
        public void setLinesRead(long linesRead) {
            this.linesRead = linesRead;
        }
        
        public long getAuditsSent() {
            return auditsSent;
        }
        
        public void setAuditsSent(long auditsSent) {
            this.auditsSent = auditsSent;
        }
        
        public long getAuditsFailed() {
            return auditsFailed;
        }
        
        public void setAuditsFailed(long auditsFailed) {
            this.auditsFailed = auditsFailed;
        }
        
        public boolean isEncounteredError() {
            return encounteredError;
        }
        
        public void setEncounteredError(boolean encounteredError) {
            this.encounteredError = encounteredError;
        }
        
        @Override
        public String toString() {
            return "{path:" + path + ", state:" + state + ", linesRead:" + linesRead + ", auditsSent:" + auditsSent + ", auditsFailed" + auditsFailed
                            + ", encounteredError:" + encounteredError + "}";
        }
    }
}
