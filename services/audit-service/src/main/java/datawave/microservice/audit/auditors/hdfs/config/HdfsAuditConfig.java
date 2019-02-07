package datawave.microservice.audit.auditors.hdfs.config;

import datawave.microservice.audit.auditors.hdfs.HdfsAuditor;
import datawave.microservice.audit.config.AuditProperties;
import datawave.webservice.common.audit.Auditor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Configures an HdfsAuditor to process messages received by the audit service in the case that our messaging infrastructure has failed. This configuration is
 * activated via the 'audit.hdfs.enabled' property.
 *
 */
@Configuration
@ConditionalOnProperty(name = "audit.auditors.hdfs.enabled", havingValue = "true")
public class HdfsAuditConfig {
    
    @Bean("hdfsAuditProperties")
    @ConfigurationProperties("audit.auditors.hdfs")
    public HdfsAuditProperties hdfsAuditProperties() {
        return new HdfsAuditProperties();
    }
    
    @Bean(name = "hdfsAuditor")
    public Auditor hdfsAuditor(AuditProperties auditProperties, @Qualifier("hdfsAuditProperties") HdfsAuditProperties hdfsAuditProperties) throws Exception {
        String hdfsUri = (hdfsAuditProperties.getHdfsUri() != null) ? hdfsAuditProperties.getHdfsUri() : auditProperties.getHdfs().getHdfsUri();
        List<String> configResources = (hdfsAuditProperties.getConfigResources() != null) ? hdfsAuditProperties.getConfigResources()
                        : auditProperties.getHdfs().getConfigResources();
        
        return new HdfsAuditor.Builder().setHdfsUri(hdfsUri).setPath(hdfsAuditProperties.getPath())
                        .setMaxFileAgeMillis(hdfsAuditProperties.getMaxFileAgeMillis()).setMaxFileLenBytes(hdfsAuditProperties.getMaxFileLenBytes())
                        .setConfigResources(configResources).setPrefix(hdfsAuditProperties().getPrefix()).build();
    }
}
