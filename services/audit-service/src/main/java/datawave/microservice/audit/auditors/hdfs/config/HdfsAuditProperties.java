package datawave.microservice.audit.auditors.hdfs.config;

import datawave.microservice.audit.config.AuditProperties;

public class HdfsAuditProperties extends AuditProperties.Hdfs {
    
    private String path;
    private String prefix;
    private long maxFileLenBytes;
    private long maxFileAgeMillis;
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public String getPrefix() {
        return prefix;
    }
    
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    public long getMaxFileLenBytes() {
        return maxFileLenBytes;
    }
    
    public void setMaxFileLenBytes(long maxFileLenBytes) {
        this.maxFileLenBytes = maxFileLenBytes;
    }
    
    public long getMaxFileAgeMillis() {
        return maxFileAgeMillis;
    }
    
    public void setMaxFileAgeMillis(long maxFileAgeMillis) {
        this.maxFileAgeMillis = maxFileAgeMillis;
    }
}
