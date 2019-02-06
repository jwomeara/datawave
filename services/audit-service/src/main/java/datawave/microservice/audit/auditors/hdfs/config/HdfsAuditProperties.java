package datawave.microservice.audit.auditors.hdfs.config;

import java.util.List;

public class HdfsAuditProperties {
    
    private String hdfsUri;
    private String path;
    private String prefix;
    private long maxFileLenBytes;
    private long maxFileAgeMillis;
    private List<String> configResources;
    
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
    
    public List<String> getConfigResources() {
        return configResources;
    }
    
    public void setConfigResources(List<String> configResources) {
        this.configResources = configResources;
    }
}
