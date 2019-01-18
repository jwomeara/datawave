package datawave.microservice.audit.dump.config;

import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.common.AuditMessageHandler;
import datawave.microservice.audit.dump.DumpAuditor;
import datawave.microservice.audit.hdfs.HdfsAuditor;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
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
@EnableConfigurationProperties(AuditDumpProperties.class)
@EnableBinding(AuditDumpConfig.AuditDumpBinding.class)
@ConditionalOnProperty(name = "audit.dump.enabled", havingValue = "true")
public class AuditDumpConfig {
    
    @Resource(name = "msgHandlerAuditParams")
    private AuditParameters msgHandlerAuditParams;
    
    @Bean
    public AuditMessageHandler auditDumpMessageHandler(Auditor dumpAuditor) {
        return new AuditMessageHandler(msgHandlerAuditParams, dumpAuditor) {
            @Override
            @StreamListener(AuditDumpConfig.AuditDumpBinding.NAME)
            public void onMessage(AuditMessage msg) throws Exception {
                super.onMessage(msg);
            }
        };
    }
    
    @Bean
    public Auditor dumpAuditor(AuditDumpProperties auditDumpProperties) throws Exception {
        return new DumpAuditor.Builder().setHdfsUri(auditDumpProperties.getHdfsUri()).setPath(auditDumpProperties.getPath())
                .setCodecName(auditDumpProperties.getCodecName()).setMaxFileAgeMillis(auditDumpProperties.getMaxFileAgeMillis())
                .setMaxFileLenBytes(auditDumpProperties.getMaxFileLenBytes()).build();
    }
    
    public interface AuditDumpBinding {
        String NAME = "auditDumpSink";
        
        @Input(NAME)
        SubscribableChannel auditDumpSink();
    }
}
