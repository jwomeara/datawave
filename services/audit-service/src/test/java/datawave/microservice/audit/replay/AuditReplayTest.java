package datawave.microservice.audit.replay;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.config.AuditProperties;
import datawave.microservice.audit.config.AuditServiceConfig;
import datawave.microservice.audit.replay.config.ReplayProperties;
import datawave.microservice.audit.replay.status.Status;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.audit.Auditor;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = AuditReplayTest.AuditReplayTestConfiguration.class)
@ActiveProfiles({"AuditReplayTest", "replay-config"})
public class AuditReplayTest {

    @LocalServerPort
    private int webServicePort;

    @Autowired
    private RestTemplateBuilder restTemplateBuilder;

    private JWTRestTemplate jwtRestTemplate;

    @Autowired
    private MessageCollector messageCollector;

    @Autowired
    private AuditServiceConfig.AuditSourceBinding auditSourceBinding;

    @Autowired
    private AuditProperties auditProperties;

    @Autowired
    private ReplayProperties replayProperties;

    private static File tempDir;

    private SubjectIssuerDNPair DN;
    private String userDN = "userDn";
    private String query = "some query";
    private String authorizations = "AUTH1,AUTH2";
    private Auditor.AuditType auditType = Auditor.AuditType.ACTIVE;

    @BeforeClass
    public static void classSetup() {
        // create a temp dir for each test
        tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
    }

    // Before method clears temp dir, then writes multiple files to temp dir, of varying states
    @Before
    public void setup() throws Exception {
        FileUtils.cleanDirectory(tempDir);

        // Copy replay files before each test
        File dataDir = new File("src/test/resources/data");
        for (File file : dataDir.listFiles())
            FileUtils.copyFileToDirectory(file, tempDir);

        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
    }

    // Test unauthorized user
    @Test(expected = HttpClientErrorException.class)
    public void unauthorizedUserTest() {
        Collection<String> roles = Collections.singleton("AuthorizedUser");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());

        UriComponents uri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/replay/createAndStart")
                .queryParam("path", tempDir.getAbsolutePath()).build();

        jwtRestTemplate.exchange(authUser, HttpMethod.POST, uri, String.class);
    }

    // Test create (0 msgs/sec), status, stop, status, update, status, resume, verify status, delete, verify file names, verify messageCollector
    @Test
    public void singleAuditReplayIgnoreUnfinishedTest() throws Exception {
        Collection<String> roles = Collections.singleton("Administrator");
        DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
        ProxiedUserDetails authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());

        UriComponents createUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/replay/create")
                .queryParam("path", tempDir.getAbsolutePath()).queryParam("sendRate", 0).build();

        // Create the audit replay request
        ResponseEntity<String> response = jwtRestTemplate.exchange(authUser, HttpMethod.POST, createUri, String.class);
        String replayId = response.getBody();

        // Get the status of the audit replay request
        UriComponents statusUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/replay/" + replayId + "/status").build();
        Status status = toStatus(jwtRestTemplate.exchange(authUser, HttpMethod.GET, statusUri, String.class));

        Assert.assertEquals(Status.ReplayState.CREATED, status.getState());
        Assert.assertEquals("file:///", status.getFileUri());
        Assert.assertEquals(tempDir.getAbsolutePath(), status.getPath());
        Assert.assertEquals(0L, status.getSendRate());
        Assert.assertEquals(0, status.getFiles().size());
        Assert.assertEquals(false, status.isReplayUnfinishedFiles());

        // Start the audit replay request
        UriComponents startUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/replay/" + replayId + "/start").build();
        ResponseEntity<String> startResp = jwtRestTemplate.exchange(authUser, HttpMethod.POST, startUri, String.class);

        Assert.assertEquals(200, startResp.getStatusCode().value());

        // Get the status of the audit replay request
        status = toStatus(jwtRestTemplate.exchange(authUser, HttpMethod.GET, statusUri, String.class));

        int numRetries = 5;
        while(status.getFiles().size() <= 0 && numRetries-- > 0) {
            Thread.sleep(100);
            status = toStatus(jwtRestTemplate.exchange(authUser, HttpMethod.GET, statusUri, String.class));
        }

        Assert.assertEquals(Status.ReplayState.RUNNING, status.getState());
        Assert.assertEquals("file:///", status.getFileUri());
        Assert.assertEquals(tempDir.getAbsolutePath(), status.getPath());
        Assert.assertEquals(0L, status.getSendRate());
        Assert.assertEquals(2, status.getFiles().size());

        Status.FileStatus fileStatus = status.getFiles().get(0);
        Assert.assertEquals(Status.FileState.RUNNING,  fileStatus.getState());
        Assert.assertTrue(fileStatus.getPath().endsWith("_RUNNING.audit-20190227_000000.000.json"));
        Assert.assertEquals(0, fileStatus.getLinesRead());
        Assert.assertEquals(0, fileStatus.getAuditsSent());
        Assert.assertEquals(0, fileStatus.getAuditsFailed());
        Assert.assertEquals(false, fileStatus.isEncounteredError());

        fileStatus = status.getFiles().get(1);
        Assert.assertEquals(Status.FileState.QUEUED,  fileStatus.getState());
        Assert.assertTrue(fileStatus.getPath().endsWith("_QUEUED.audit-20190228_000000.000.json"));
        Assert.assertEquals(0, fileStatus.getLinesRead());
        Assert.assertEquals(0, fileStatus.getAuditsSent());
        Assert.assertEquals(0, fileStatus.getAuditsFailed());
        Assert.assertEquals(false, fileStatus.isEncounteredError());

        Assert.assertEquals(false, status.isReplayUnfinishedFiles());

        // Stop the audit replay request
        UriComponents stopUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/replay/" + replayId + "/stop").build();
        ResponseEntity<String> stopResp = jwtRestTemplate.exchange(authUser, HttpMethod.POST, stopUri, String.class);

        Assert.assertEquals(200, stopResp.getStatusCode().value());

        // Get the status of the audit replay request
        status = toStatus(jwtRestTemplate.exchange(authUser, HttpMethod.GET, statusUri, String.class));

        Assert.assertEquals(Status.ReplayState.STOPPED, status.getState());
        Assert.assertEquals("file:///", status.getFileUri());
        Assert.assertEquals(tempDir.getAbsolutePath(), status.getPath());
        Assert.assertEquals(0L, status.getSendRate());
        Assert.assertEquals(2, status.getFiles().size());

        fileStatus = status.getFiles().get(0);
        Assert.assertEquals(Status.FileState.RUNNING,  fileStatus.getState());
        Assert.assertTrue(fileStatus.getPath().endsWith("_RUNNING.audit-20190227_000000.000.json"));
        Assert.assertEquals(0, fileStatus.getLinesRead());
        Assert.assertEquals(0, fileStatus.getAuditsSent());
        Assert.assertEquals(0, fileStatus.getAuditsFailed());
        Assert.assertEquals(false, fileStatus.isEncounteredError());

        fileStatus = status.getFiles().get(1);
        Assert.assertEquals(Status.FileState.QUEUED,  fileStatus.getState());
        Assert.assertTrue(fileStatus.getPath().endsWith("_QUEUED.audit-20190228_000000.000.json"));
        Assert.assertEquals(0, fileStatus.getLinesRead());
        Assert.assertEquals(0, fileStatus.getAuditsSent());
        Assert.assertEquals(0, fileStatus.getAuditsFailed());
        Assert.assertEquals(false, fileStatus.isEncounteredError());

        Assert.assertEquals(false, status.isReplayUnfinishedFiles());

        // Update the audit replay request
        UriComponents updateUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/replay/" + replayId + "/update")
                .queryParam("sendRate", 200).build();
        ResponseEntity<String> updateResp = jwtRestTemplate.exchange(authUser, HttpMethod.POST, updateUri, String.class);

        Assert.assertEquals(200, updateResp.getStatusCode().value());

        // Get the status of the audit replay request
        status = toStatus(jwtRestTemplate.exchange(authUser, HttpMethod.GET, statusUri, String.class));

        Assert.assertEquals(Status.ReplayState.STOPPED, status.getState());
        Assert.assertEquals("file:///", status.getFileUri());
        Assert.assertEquals(tempDir.getAbsolutePath(), status.getPath());
        Assert.assertEquals(200L, status.getSendRate());
        Assert.assertEquals(2, status.getFiles().size());

        fileStatus = status.getFiles().get(0);
        Assert.assertEquals(Status.FileState.RUNNING,  fileStatus.getState());
        Assert.assertTrue(fileStatus.getPath().endsWith("_RUNNING.audit-20190227_000000.000.json"));
        Assert.assertEquals(0, fileStatus.getLinesRead());
        Assert.assertEquals(0, fileStatus.getAuditsSent());
        Assert.assertEquals(0, fileStatus.getAuditsFailed());
        Assert.assertEquals(false, fileStatus.isEncounteredError());

        fileStatus = status.getFiles().get(1);
        Assert.assertEquals(Status.FileState.QUEUED,  fileStatus.getState());
        Assert.assertTrue(fileStatus.getPath().endsWith("_QUEUED.audit-20190228_000000.000.json"));
        Assert.assertEquals(0, fileStatus.getLinesRead());
        Assert.assertEquals(0, fileStatus.getAuditsSent());
        Assert.assertEquals(0, fileStatus.getAuditsFailed());
        Assert.assertEquals(false, fileStatus.isEncounteredError());

        Assert.assertEquals(false, status.isReplayUnfinishedFiles());

        // Resume the audit replay request
        UriComponents resumeUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/replay/" + replayId + "/resume").build();
        ResponseEntity<String> resumeResp = jwtRestTemplate.exchange(authUser, HttpMethod.POST, resumeUri, String.class);

        Assert.assertEquals(200, resumeResp.getStatusCode().value());

        // Check the status until it is not running
        status = toStatus(jwtRestTemplate.exchange(authUser, HttpMethod.GET, statusUri, String.class));
        numRetries = 5;
        while(status.getState() == Status.ReplayState.RUNNING && numRetries-- != 0) {
            Thread.sleep(50);
            status = toStatus(jwtRestTemplate.exchange(authUser, HttpMethod.GET, statusUri, String.class));
        }

        Assert.assertEquals(Status.ReplayState.FINISHED, status.getState());
        Assert.assertEquals("file:///", status.getFileUri());
        Assert.assertEquals(tempDir.getAbsolutePath(), status.getPath());
        Assert.assertEquals(200L, status.getSendRate());
        Assert.assertEquals(2, status.getFiles().size());

        fileStatus = status.getFiles().get(0);
        Assert.assertEquals(Status.FileState.FINISHED,  fileStatus.getState());
        Assert.assertTrue(fileStatus.getPath().endsWith("_FINISHED.audit-20190227_000000.000.json"));
        Assert.assertEquals(1, fileStatus.getLinesRead());
        Assert.assertEquals(1, fileStatus.getAuditsSent());
        Assert.assertEquals(0, fileStatus.getAuditsFailed());
        Assert.assertEquals(false, fileStatus.isEncounteredError());

        fileStatus = status.getFiles().get(1);
        Assert.assertEquals(Status.FileState.FINISHED,  fileStatus.getState());
        Assert.assertTrue(fileStatus.getPath().endsWith("_FINISHED.audit-20190228_000000.000.json"));
        Assert.assertEquals(1, fileStatus.getLinesRead());
        Assert.assertEquals(1, fileStatus.getAuditsSent());
        Assert.assertEquals(0, fileStatus.getAuditsFailed());
        Assert.assertEquals(false, fileStatus.isEncounteredError());

        Assert.assertEquals(false, status.isReplayUnfinishedFiles());

        // Delete the audit replay request
        UriComponents deleteUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/audit/v1/replay/" + replayId + "/delete").build();
        ResponseEntity<String> deleteResp = jwtRestTemplate.exchange(authUser, HttpMethod.POST, deleteUri, String.class);

        Assert.assertEquals(200, deleteResp.getStatusCode().value());

        Exception exception = null;
        try {
            jwtRestTemplate.exchange(authUser, HttpMethod.GET, statusUri, String.class);

        } catch (Exception e) {
            exception = e;
        }

        Assert.assertNotNull(exception);
        Assert.assertTrue(exception instanceof HttpClientErrorException);

        // Verify the message collector has our audit messages
        @SuppressWarnings("unchecked")
        Message<AuditMessage> msg = (Message<AuditMessage>) messageCollector.forChannel(auditSourceBinding.auditSource()).poll();
        assertNotNull(msg);
        Map<String,String> received = msg.getPayload().getAuditParameters();

        @SuppressWarnings("unchecked")
        Message<AuditMessage> msg2 = (Message<AuditMessage>) messageCollector.forChannel(auditSourceBinding.auditSource()).poll();
        assertNotNull(msg2);
        received = msg2.getPayload().getAuditParameters();

        System.out.println("done");


    }


    // Test multiple create (0 msgs/sec), stopAll, updateAll, resumeAll, verify statusAll, deleteAll, verify file names, verify messageCollector
    public void multipleAuditReplaysTest() {

    }


    // Test createAndStart, verify status, verify file contents

    // Test separate create and start, verify statusAll, verify file names, verify messageCollector

    // Test multiple create and startAll, verify statusAll, verify file names, verify messageCollector

    // Test create and update, verify status

    // Test multiple create and updateAll, verify statusAll

    // Test create (0 msgs/sec), start, and stop, verify status

    // Test multiple create (0 msgs/sec), startAll, stopAll, verify status

    // Test create, verify status, delete, verify status

    // Test multiple create, verify statusAll, deleteAll, verify statusAll

    // Test idle timeout

    // Test replay of unfinished filed

    // Test delete on running replay

    private static Status toStatus(ResponseEntity<String> responseEntity) throws IOException {
        return toStatus(responseEntity.getBody());
    }

    private static ObjectMapper objectMapper = new ObjectMapper();
    private static Status toStatus(String stringStatus) throws IOException {
        Status status = new Status();
        Map<String,Object> map = objectMapper.readValue(stringStatus, new TypeReference<HashMap<String,Object>>() {});
        status.setId((String)map.get("id"));
        status.setState(Status.ReplayState.valueOf((String)map.get("state")));
        status.setFileUri((String)map.get("fileUri"));
        status.setPath((String)map.get("path"));
        status.setSendRate(Integer.toUnsignedLong((int)map.get("sendRate")));
        status.setLastUpdated(new Date((long)map.get("lastUpdated")));
        status.setReplayUnfinishedFiles((boolean)map.get("replayUnfinishedFiles"));

        if (map.get("files") != null) {
            List<Status.FileStatus> fileStatuses = new ArrayList<>();
            for(Map<String,Object> file : (List<Map<String,Object>>)map.get("files")) {
                Status.FileState state = Status.FileState.valueOf((String)file.get("state"));
                String path = (String)file.get("path");

                Status.FileStatus fileStatus = new Status.FileStatus(path, state);
                fileStatus.setLinesRead((int)file.get("linesRead"));
                fileStatus.setAuditsSent((int)file.get("auditsSent"));
                fileStatus.setAuditsFailed((int)file.get("auditsFailed"));
                fileStatus.setEncounteredError((boolean)file.get("encounteredError"));

                fileStatuses.add(fileStatus);
            }
            status.setFiles(fileStatuses);
        }

        status.getFiles().sort((o1, o2) -> {
            if (o1.getState() == o2.getState())
                return o1.getPath().compareTo(o2.getPath());
            else
                return o2.getState().ordinal() - o1.getState().ordinal();
        });

        return status;
    }

    @Configuration
    @Profile("AuditReplayTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class AuditReplayTestConfiguration {

    }
}
