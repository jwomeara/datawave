package datawave.microservice.audit.controller;

import datawave.microservice.audit.config.AuditServiceConfig;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.annotation.security.RolesAllowed;

@RestController
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class AuditController {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private AuditParameters restAuditParams;
    
    private MessageChannel messageChannel;
    
    public AuditController(@Qualifier("restAuditParams") AuditParameters restAuditParams,
                    @Qualifier(AuditServiceConfig.AuditSourceBinding.NAME) MessageChannel messageChannel) {
        this.restAuditParams = restAuditParams;
        this.messageChannel = messageChannel;
    }
    
    private void sendMessage(AuditParameters parameters) throws Exception {
        messageChannel.send(MessageBuilder.withPayload(parameters.toMap()).build());
    }
    
    @RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/audit", method = RequestMethod.POST)
    public VoidResponse audit(@RequestParam MultiValueMap<String,String> parameters) {
        VoidResponse response = new VoidResponse();
        try {
            restAuditParams.clear();
            restAuditParams.validate(parameters);
            sendMessage(restAuditParams);
            return response;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.AUDITING_ERROR, e);
            log.error(qe.toString());
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
}
