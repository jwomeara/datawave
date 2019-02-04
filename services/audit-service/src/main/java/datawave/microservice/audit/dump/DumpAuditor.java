package datawave.microservice.audit.dump;

import datawave.microservice.audit.hdfs.HdfsAuditor;
import datawave.webservice.common.audit.Auditor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * An implementation for {@link Auditor}, which writes URL encoded, JSON formatted audit messages to a file in HDFS.
 * The dump auditor adds functionality which will automatically close the open file handle after the specified amount
 * of time has elapsed without receiving any new audit messages.
 */
public class DumpAuditor extends HdfsAuditor {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private long updateTimeoutMillis;

    private Timer lastUpdateTimer = null;

    private boolean auditsReceived = false;

    protected DumpAuditor(Builder builder) throws URISyntaxException, IOException {
        super(builder);
        updateTimeoutMillis = builder.updateTimeoutMillis;
    }

    @Override
    protected void writeAudit(String jsonAuditParams) throws Exception {
        // setup the last update timer
        if (lastUpdateTimer == null) {
            lastUpdateTimer = new Timer();
            lastUpdateTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    if (!auditsReceived) {
                        try {
                            close();
                        } catch (IOException e) {
                            log.warn("Unable to close the audit dump file.", e);
                        }

                        lastUpdateTimer.cancel();
                        lastUpdateTimer = null;
                    }
                    auditsReceived = false;
                }
            },
            updateTimeoutMillis,
            updateTimeoutMillis);
        }

        auditsReceived = true;
        super.writeAudit(jsonAuditParams);
    }

    @PreDestroy
    @Override
    protected void destroy() throws Exception {
        super.destroy();
    }

    public static class Builder extends HdfsAuditor.Builder {
        protected long updateTimeoutMillis;

        public Builder() {
            prefix = "dump";
            codecName = null;
            maxFileLenBytes = 1024L * 1024L * 512L;
            maxFileAgeMillis = TimeUnit.MINUTES.toMillis(30);
            updateTimeoutMillis = TimeUnit.MINUTES.toMillis(1);
        }

        public long getUpdateTimeoutMillis() {
            return updateTimeoutMillis;
        }

        public Builder setUpdateTimeoutMillis(long updateTimeoutMillis) {
            this.updateTimeoutMillis = updateTimeoutMillis;
            return this;
        }

        public DumpAuditor build() throws IOException, URISyntaxException {
            return new DumpAuditor(this);
        }
    }
}
