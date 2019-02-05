package datawave.microservice.audit.dump.config;

import datawave.microservice.audit.hdfs.config.HdfsAuditProperties;

public class AuditDumpProperties extends HdfsAuditProperties {
    protected long updateTimeoutMillis;
    
    public long getUpdateTimeoutMillis() {
        return updateTimeoutMillis;
    }
    
    public void setUpdateTimeoutMillis(long updateTimeoutMillis) {
        this.updateTimeoutMillis = updateTimeoutMillis;
    }
}
