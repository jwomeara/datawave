package datawave.microservice.audit.hdfs.config;

import datawave.microservice.audit.hdfs.HdfsAuditor;
import datawave.webservice.common.audit.Auditor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures an HdfsAuditor to process messages received by the audit service in the case that our messaging infrastructure has failed. This configuration is
 * activated via the 'audit.hdfs.enabled' property.
 *
 */
@Configuration
@ConditionalOnProperty(name = "audit.hdfs.enabled", havingValue = "true")
public class HdfsAuditConfig {
    
    @Bean("hdfsAuditProperties")
    @ConfigurationProperties("audit.hdfs")
    public HdfsAuditProperties hdfsAuditProperties() {
        return new HdfsAuditProperties();
    }
    
    @Bean("hdfsAuditor")
    public Auditor hdfsAuditor(@Qualifier("hdfsAuditProperties") HdfsAuditProperties hdfsAuditProperties) throws Exception {
        return new HdfsAuditor.Builder().setHdfsUri(hdfsAuditProperties.getHdfsUri()).setPath(hdfsAuditProperties.getPath())
                        .setCodecName(hdfsAuditProperties.getCodecName()).setMaxFileAgeMillis(hdfsAuditProperties.getMaxFileAgeMillis())
                        .setMaxFileLenBytes(hdfsAuditProperties.getMaxFileLenBytes()).setConfigResources(hdfsAuditProperties.getConfigResources()).build();
    }
}
