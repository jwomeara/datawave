package datawave.microservice.audit.auditors.dump.config;

import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.common.AuditMessageHandler;
import datawave.microservice.audit.auditors.hdfs.HdfsAuditor;
import datawave.microservice.audit.auditors.hdfs.config.HdfsAuditProperties;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

import javax.annotation.Resource;

/**
 * Configures an HdfsAuditor to dump messages to HDFS by request. This configuration is activated via the 'audit.dump.enabled' property. When enabled, this
 * configuration will also enable the appropriate Spring Cloud Stream configuration for the audit dump binding, as specified in the audit config.
 */
@Configuration
@EnableBinding(DumpAuditConfig.AuditDumpBinding.class)
@ConditionalOnProperty(name = "audit.dump.enabled", havingValue = "true")
public class DumpAuditConfig {
    
    @Bean("dumpAuditProperties")
    @ConfigurationProperties("audit.dump")
    public HdfsAuditProperties dumpAuditProperties() {
        return new HdfsAuditProperties();
    }
    
    @Resource(name = "msgHandlerAuditParams")
    private AuditParameters msgHandlerAuditParams;
    
    @Bean
    public AuditMessageHandler auditDumpMessageHandler(Auditor dumpAuditor) {
        return new AuditMessageHandler(msgHandlerAuditParams, dumpAuditor) {
            @Override
            @StreamListener(DumpAuditConfig.AuditDumpBinding.NAME)
            public void onMessage(AuditMessage msg) throws Exception {
                super.onMessage(msg);
            }
        };
    }
    
    @Bean
    public Auditor dumpAuditor(@Qualifier("dumpAuditProperties") HdfsAuditProperties dumpAuditProperties) throws Exception {
        return new HdfsAuditor.Builder().setHdfsUri(dumpAuditProperties.getHdfsUri()).setPath(dumpAuditProperties.getPath())
                        .setMaxFileAgeMillis(dumpAuditProperties.getMaxFileAgeMillis()).setMaxFileLenBytes(dumpAuditProperties.getMaxFileLenBytes())
                        .setConfigResources(dumpAuditProperties.getConfigResources()).setPrefix(dumpAuditProperties().getPrefix()).build();
    }
    
    public interface AuditDumpBinding {
        String NAME = "dumpAuditSink";
        
        @Input(NAME)
        SubscribableChannel auditDumpSink();
    }
}
