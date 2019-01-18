package datawave.microservice.audit.hdfs.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.concurrent.TimeUnit;

@EnableConfigurationProperties(HdfsAuditProperties.class)
@ConfigurationProperties(prefix = "audit.hdfs")
public class HdfsAuditProperties {
    
    private String hdfsUri;
    private String path;
    private String prefix = "audits";
    private String codecName = null;
    private long maxFileLenBytes = 1024L * 1024L * 512L;
    private long maxFileAgeMillis = TimeUnit.MINUTES.toMillis(30);
    
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
    
    public String getCodecName() {
        return codecName;
    }
    
    public void setCodecName(String codecName) {
        this.codecName = codecName;
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
