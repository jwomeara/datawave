package datawave.microservice.audit.replay;

import java.util.Date;
import java.util.List;

public class ReplayStatus {

    public enum ReplayState {
        RUNNING, STOPPED, FINISHED, FAILED
    }

    public enum FileState {
        QUEUED, REPLAYING, REPLAYED, FINISHED, FAILED
    }

    private String id;
    private ReplayState state;
    private String hdfsUri;
    private String path;
    private long sendRate;
    private List<FileStatus> files;
    private Date lastUpdated;

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

    public String getHdfsUri() {
        return hdfsUri;
    }

    public void setHdfsUri(String hdfsUri) {
        this.hdfsUri = hdfsUri;
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

    public static final class FileStatus {
        private FileState state;
        private String path;
        private List<String> auditIds;

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

        public List<String> getAuditIds() {
            return auditIds;
        }

        public void setAuditIds(List<String> auditIds) {
            this.auditIds = auditIds;
        }
    }
}
