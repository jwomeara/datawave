package datawave.microservice.audit;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.*;

public class AuditTest {
    
    public static void main(String[] args) throws Exception {

        //default truststore parameters
        System.setProperty("javax.net.ssl.trustStore", "~/projects/datawave/services/common/src/main/resources/testCA.p12");
        System.setProperty("javax.net.ssl.trustStorePassword", "ChangeIt");
        System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");

        //my certificate and password
        System.setProperty("javax.net.ssl.keyStore", "~/projects/datawave/services/common/src/main/resources/testUser.p12");
        System.setProperty("javax.net.ssl.keyStorePassword", "ChangeIt");
        System.setProperty("javax.net.ssl.keyStoreType", "PKCS12");

        String token = "eyJhbGciOiJSUzUxMiIsInppcCI6IkdaSVAifQ.H4sIAAAAAAAAAL1SXUvDMBT9KyHPcdgydRtWqNtAhW1gW32QMWJ60UibhCQd07H_7k23ydA9CIJv4eTc83GTNXXNMx1QoRIPzhPeIY0Dy4huEljx2lRASlhCpQ1Yh_AXKrQ1jIikcZc4fIgSwY8TryijvCnRbpTm6WP6MEZAOofAcNr6o_USbGc32hG6ZmRWJFkLo_0sGe9Uh63qMCky1ICVoYPoLO5enPZ6UZ9RY6US0vAKtZ_WtFR0sA5N30D40fSPfbehG7B7pV-VpxtGg1X-bgDHimx8367Dv4aM9O56lmWLdDS5nSKcFvnNoqXMGbW6gh1HO5eWtVTSecu9toGKCtrKDygLVN_zcx3wCTdGqpdQ_sjsN9f5Dy0kHATB_MIC91KrXNawXfh5HHejfhz320eQ9uD6JGJU8XD6r9-1mW8-AfgaOz_QAgAA.AUHzwu7xFYd-7iX0gVNDEJds4x-AVG3GXrPlZF6UxqZlgsOCO441sxRRt5WgjXCJGZWH44nolKAw5-hTjBl_5WtEXmd3zSUNwI45TgaDbGTn3k5FttoWtmtnfytwcMepNku35pPRTwbdxdbuHUYBk2dK83vQFN7bSu-eX5ybG2N9lEVhkzmyOFKWHG7X-qchPyCCNaQLCKMs65-YvRDoF47cVEhKLs7u1iEw8aIMY8GOS1ofo44BUb_pezJch4W1S_BErlk2tIDyhoOl3iAEM2rOb8WD-WGC2Rbk6OkrS6ZOtimuYkcTYSJWwqm3108hS6ZjU816PTVLav7nGU1VFQ";
        
        AuditParameters auditParams = new AuditParameters();
        Map<String,List<String>> params = new HashMap<>();
        params.put(AuditParameters.USER_DN, Arrays.asList("someUser"));
        params.put(AuditParameters.QUERY_STRING, Arrays.asList("someQuery"));
        params.put(AuditParameters.QUERY_SELECTORS, Arrays.asList("sel1", "sel2"));
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, Arrays.asList("AUTH1,AUTH2"));
        params.put(AuditParameters.QUERY_AUDIT_TYPE, Arrays.asList(Auditor.AuditType.ACTIVE.name()));
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Arrays.asList("ALL"));
        params.put(AuditParameters.QUERY_DATE, Arrays.asList(Long.toString(new Date().getTime())));
        auditParams.validate(params);
        
        HttpClient client = HttpClients.createSystem();
        
        Map<String,String> theParams = auditParams.toMap();
        
        URIBuilder builder = new URIBuilder();
        builder.setScheme("https");
        builder.setHost("localhost");
        builder.setPort(8743);
        builder.setPath("/audit/audit");

        for (String param : auditParams.toMap().keySet())
            builder.addParameter(param, theParams.get(param));
        
        HttpPost post = new HttpPost(builder.build());
        
        post.setHeader("Authorization", "Bearer " + token);
        
        HttpResponse resp = client.execute(post);
        
        if (resp.getStatusLine().getStatusCode() != 200) {
            throw new ClientProtocolException("Unable to post: " + resp.getStatusLine() + " " + EntityUtils.toString(resp.getEntity()));
        } else {
            System.out.println(resp.getStatusLine());
        }
    }
}
