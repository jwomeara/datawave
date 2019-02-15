package datawave.microservice.audit;

import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.config.AuditProperties;
import datawave.microservice.audit.config.AuditProperties.Retry;
import datawave.microservice.audit.config.AuditServiceConfig;
import datawave.microservice.audit.health.HealthChecker;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static datawave.microservice.audit.config.AuditServiceConfig.CONFIRM_ACK_CHANNEL;
import static datawave.webservice.common.audit.AuditParameters.AUDIT_ID;
import static datawave.webservice.common.audit.AuditParameters.QUERY_AUDIT_TYPE;
import static datawave.webservice.common.audit.AuditParameters.QUERY_AUTHORIZATIONS;
import static datawave.webservice.common.audit.AuditParameters.QUERY_DATE;
import static datawave.webservice.common.audit.AuditParameters.QUERY_LOGIC_CLASS;
import static datawave.webservice.common.audit.AuditParameters.QUERY_SECURITY_MARKING_COLVIZ;
import static datawave.webservice.common.audit.AuditParameters.QUERY_SELECTORS;
import static datawave.webservice.common.audit.AuditParameters.QUERY_STRING;
import static datawave.webservice.common.audit.AuditParameters.USER_DN;

/**
 * The AuditController presents the REST endpoints for the audit service.
 * <p>
 * Before returning success to the caller, the audit controller will verify that the audit message was successfully passed to our messaging infrastructure.
 * Also, if configured, the audit controller will verify that the message passing infrastructure is healthy before returning successfully to the user. If the
 * message passing infrastructure is unhealthy, or if we can't verify that the message was successfully passed to our messaging infrastructure, a 500 Internal
 * Server Error will be returned to the caller.
 */
@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class AuditController {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final AuditProperties auditProperties;
    
    private final AuditParameters restAuditParams;
    
    private final MessageChannel messageChannel;

    @Autowired(required = false)
    private HealthChecker healthChecker;
    
    @Autowired(required = false)
    @Qualifier("hdfsAuditor")
    private Auditor hdfsAuditor;
    
    private static final Map<String,CountDownLatch> correlationLatchMap = new ConcurrentHashMap<>();
    
    public AuditController(AuditProperties auditProperties, @Qualifier("restAuditParams") AuditParameters restAuditParams,
                    @Qualifier(AuditServiceConfig.AuditSourceBinding.NAME) MessageChannel messageChannel) {
        this.auditProperties = auditProperties;
        this.restAuditParams = restAuditParams;
        this.messageChannel = messageChannel;
    }
    
    /**
     * Receives producer confirm acks, and disengages the latch associated with the given correlation ID.
     * 
     * @param message
     *            the confirmation ack message
     */
    @ConditionalOnProperty(value = "audit.confirmAckEnabled", havingValue = "true", matchIfMissing = true)
    @ServiceActivator(inputChannel = CONFIRM_ACK_CHANNEL)
    public void processConfirmAck(Message<?> message) {
        Object headerObj = message.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
        
        if (headerObj != null) {
            String correlationId = headerObj.toString();
            if (correlationLatchMap.containsKey(correlationId)) {
                correlationLatchMap.get(correlationId).countDown();
            } else
                log.warn("Unable to decrement latch for audit ID [" + correlationId + "]");
        } else {
            log.warn("No correlation ID found in confirm ack message");
        }
    }
    
    /**
     * Passes audit messages to the messaging infrastructure.
     * <p>
     * The audit ID is used as a correlation ID in order to ensure that a producer confirm ack is received. If a producer confirm ack is not received within the
     * specified amount of time, a 500 Internal Server Error will be returned to the caller.
     * 
     * @param parameters
     *            The audit parameters to be sent
     */
    private boolean sendMessage(AuditParameters parameters) {
        if ((healthChecker != null && healthChecker.isHealthy()) || healthChecker == null) {
            String auditId = parameters.getAuditId();
            
            CountDownLatch latch = null;
            if (auditProperties.isConfirmAckEnabled()) {
                latch = new CountDownLatch(1);
                correlationLatchMap.put(auditId, latch);
            }
            
            boolean success = messageChannel.send(MessageBuilder.withPayload(AuditMessage.fromParams(parameters)).setCorrelationId(auditId).build());
            
            if (auditProperties.isConfirmAckEnabled()) {
                try {
                    success = success && latch.await(auditProperties.getConfirmAckTimeoutMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    success = false;
                } finally {
                    correlationLatchMap.remove(auditId);
                }
            }
            
            return success;
        }
        
        return false;
    }
    
    /**
     * Performs auditing for the given parameters, via the configured Auditors.
     *
     * @param parameters
     *            the audit parameters
     * @return an audit ID, which can be used for tracking purposes
     */
    @ApiOperation(value = "Performs auditing for the given parameters.")
    @ApiImplicitParams({@ApiImplicitParam(name = USER_DN, required = true), @ApiImplicitParam(name = QUERY_STRING, required = true),
            @ApiImplicitParam(name = QUERY_SELECTORS), @ApiImplicitParam(name = QUERY_AUTHORIZATIONS, required = true),
            @ApiImplicitParam(name = QUERY_AUDIT_TYPE, required = true), @ApiImplicitParam(name = QUERY_SECURITY_MARKING_COLVIZ, required = true),
            @ApiImplicitParam(name = QUERY_DATE), @ApiImplicitParam(name = QUERY_LOGIC_CLASS), @ApiImplicitParam(name = AUDIT_ID)})
    @RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/audit", method = RequestMethod.POST)
    public String audit(@RequestParam MultiValueMap<String,String> parameters) {
        restAuditParams.clear();
        restAuditParams.validate(parameters);

        if (!audit(restAuditParams))
            throw new RuntimeException("Unable to process audit message with id [" + restAuditParams.getAuditId() + "]");

        return restAuditParams.getAuditId();
    }

    public boolean audit(AuditParameters auditParameters) {
        log.info("[{}] Received audit request with parameters {}", restAuditParams.getAuditId(), restAuditParams);

        boolean success;
        final long auditStartTime = System.currentTimeMillis();
        long currentTime;
        int attempts = 0;

        Retry retry = auditProperties.getRetry();

        do {
            if (attempts++ > 0) {
                try {
                    Thread.sleep(retry.getBackoffIntervalMillis());
                } catch (InterruptedException e) {
                    // Ignore -- we'll just end up retrying a little too fast
                }
            }

            if (log.isDebugEnabled())
                log.debug("[" + restAuditParams.getAuditId() + "] Audit attempt " + attempts + " of " + retry.getMaxAttempts());

            success = sendMessage(restAuditParams);
            currentTime = System.currentTimeMillis();
        } while (!success && (currentTime - auditStartTime) < retry.getFailTimeoutMillis() && attempts < retry.getMaxAttempts());

        // last ditch effort to write the audit message to hdfs for subsequent processing
        if (!success && hdfsAuditor != null) {
            success = true;
            try {
                if (log.isDebugEnabled())
                    log.debug("[" + restAuditParams.getAuditId() + "] Attempting to log audit to HDFS");

                hdfsAuditor.audit(restAuditParams);
            } catch (Exception e) {
                log.error("[" + restAuditParams.getAuditId() + "] Unable to save audit to HDFS", e);
                success = false;
            }
        }

        if (!success)
            log.warn("[" + restAuditParams.getAuditId() + "] Audit failed. {attempts = " + attempts + ", elapsedMillis = " + (currentTime - auditStartTime)
                    + ((hdfsAuditor != null) ? ", hdfsElapsedMillis = " + (System.currentTimeMillis() - currentTime) + "}" : "}"));
        else
            log.info("[" + restAuditParams.getAuditId() + "] Audit successful. {attempts = " + attempts + ", elapsedMillis = " + (currentTime - auditStartTime)
                    + ((hdfsAuditor != null) ? ", hdfsElapsedMillis = " + (System.currentTimeMillis() - currentTime) + "}" : "}"));

        return success;
    }
}
