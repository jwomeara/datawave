package datawave.microservice.audit.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation for {@link Auditor}, which writes URL encoded, JSON formatted audit messages to a file in HDFS.
 */
public class HdfsAuditor implements Auditor {
    
    protected static final ObjectMapper mapper = new ObjectMapper();

    protected final SimpleDateFormat sdf;
    protected final ReentrantLock writeLock = new ReentrantLock(true);

    protected long maxFileLenBytes;
    protected long maxFileAgeMillis;

    protected FileSystem hdfs;
    protected Path basePath;

    protected Path currentFile = null;
    protected Date creationDate = null;
    protected OutputStream outStream = null;
    protected CompressionCodec codec = null;
    
    protected HdfsAuditor(Builder builder) throws URISyntaxException, IOException {
        this.maxFileLenBytes = builder.maxFileLenBytes;
        this.maxFileAgeMillis = builder.maxFileAgeMillis;
        
        this.sdf = new SimpleDateFormat("'." + builder.prefix + "'-yyyyMMdd_HHmmss.SSS'.json'");
        
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        config.addResource(new Path("/usr/lib/hadoop/etc/hadoop/core-site.xml"));
        config.addResource(new Path("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));
        hdfs = FileSystem.get(new URI(builder.hdfsUri), config);
        
        basePath = Path.mergePaths(new Path(builder.hdfsUri), new Path(builder.path));
        if (!hdfs.exists(basePath))
            hdfs.mkdirs(basePath);
        
        codec = new CompressionCodecFactory(config).getCodecByClassName(builder.codecName);
    }
    
    @Override
    public void audit(AuditParameters auditParameters) throws Exception {
        // convert the messages to URL-encoded JSON
        Map<String,String> auditParamsMap = auditParameters.toMap();
        String jsonAuditParams = URLEncoder.encode(mapper.writeValueAsString(auditParamsMap), "UTF8") + "\n";
        
        writeLock.lock();
        try {
            writeAudit(jsonAuditParams);
        } finally {
            writeLock.unlock();
        }
    }

    protected void writeAudit(String jsonAuditParams) throws Exception {
        getOutputStream().write(jsonAuditParams.getBytes("UTF8"));
    }

    protected OutputStream getOutputStream() throws IOException, ParseException {
        // if the file/stream is null, or the file is too old/big, create a new file & output stream
        if (outStream == null || currentFile == null || isFileTooOld() || isFileTooBig()) {

            close();
            
            // create a new file and output stream
            Date currentDate;
            do {
                currentDate = new Date();
                currentFile = new Path(basePath, sdf.format(currentDate));
                try {
                    Thread.sleep((long) (Math.random() * 10));
                } catch (InterruptedException e) {
                    // getting interrupted is ok
                }
            } while (hdfs.exists(currentFile));
            outStream = hdfs.create(currentFile);
            creationDate = currentDate;
            
            if (codec != null)
                outStream = codec.createOutputStream(outStream);
        }
        return outStream;
    }

    protected void close() throws IOException {
        writeLock.lock();

        try {
            // close the output stream, if it exists
            if (outStream != null) {
                outStream.close();
                outStream = null;
            }

            // rename the file without the '.' prefix, if it exists
            if (currentFile != null) {
                hdfs.rename(currentFile, Path.mergePaths(currentFile.getParent(), new Path(currentFile.getName().substring(1))));
                currentFile = null;
                creationDate = null;
            }
        } finally {
            writeLock.unlock();
        }
    }

    protected boolean isFileTooOld() throws ParseException {
        return currentFile == null || ((new Date().getTime() - creationDate.getTime()) >= maxFileAgeMillis);
    }
    
    protected boolean isFileTooBig() throws IOException {
        return currentFile == null || hdfs.getFileStatus(currentFile).getLen() >= maxFileLenBytes;
    }

    @PreDestroy
    protected void destroy() throws Exception {
        writeLock.lock();
        close();
    }

    public static class Builder {
        protected String hdfsUri;
        protected String path;
        protected String prefix;
        protected String codecName;
        protected long maxFileLenBytes;
        protected long maxFileAgeMillis;

        public Builder() {
            prefix = "audits";
            codecName = null;
            maxFileLenBytes = 1024L * 1024L * 512L;
            maxFileAgeMillis = TimeUnit.MINUTES.toMillis(30);
        }

        public String getHdfsUri() {
            return hdfsUri;
        }
        
        public Builder setHdfsUri(String hdfsUri) {
            this.hdfsUri = hdfsUri;
            return this;
        }
        
        public String getPath() {
            return path;
        }
        
        public Builder setPath(String path) {
            this.path = path;
            return this;
        }
        
        public String getPrefix() {
            return prefix;
        }
        
        public Builder setPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }
        
        public String getCodecName() {
            return codecName;
        }
        
        public Builder setCodecName(String codecName) {
            this.codecName = codecName;
            return this;
        }
        
        public long getMaxFileLenBytes() {
            return maxFileLenBytes;
        }
        
        public Builder setMaxFileLenBytes(long maxFileLenBytes) {
            this.maxFileLenBytes = maxFileLenBytes;
            return this;
        }
        
        public long getMaxFileAgeMillis() {
            return maxFileAgeMillis;
        }
        
        public Builder setMaxFileAgeMillis(long maxFileAgeMillis) {
            this.maxFileAgeMillis = maxFileAgeMillis;
            return this;
        }
        
        public HdfsAuditor build() throws IOException, URISyntaxException {
            return new HdfsAuditor(this);
        }
    }
}
