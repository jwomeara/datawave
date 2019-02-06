package datawave.microservice.audit.auditors.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation for {@link Auditor}, which writes URL encoded, JSON formatted audit messages to a file in HDFS.
 */
public class HdfsAuditor implements Auditor {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    protected static final ObjectMapper mapper = new ObjectMapper();
    
    protected final SimpleDateFormat sdf;
    protected final ReentrantLock writeLock = new ReentrantLock(true);
    
    protected long maxFileLenBytes;
    protected long maxFileAgeMillis;
    
    protected FileSystem hdfs;
    protected Path basePath;
    
    protected Path currentFile = null;
    protected Date creationDate = null;
    
    protected HdfsAuditor(Builder<?> builder) throws URISyntaxException, IOException {
        this.maxFileLenBytes = builder.maxFileLenBytes;
        this.maxFileAgeMillis = builder.maxFileAgeMillis;
        
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        for (String resource : builder.configResources)
            config.addResource(new Path(resource));
        
        if (builder.hdfsUri != null)
            hdfs = FileSystem.get(new URI(builder.hdfsUri), config);
        else
            hdfs = FileSystem.get(config);
        
        basePath = new Path(builder.path);
        if (!hdfs.exists(basePath))
            hdfs.mkdirs(basePath);
        
        this.sdf = new SimpleDateFormat("'" + builder.prefix + "'-yyyyMMdd_HHmmss.SSS'.json'");
    }
    
    @Override
    public void audit(AuditParameters auditParameters) throws Exception {
        
        // convert the messages to URL-encoded JSON
        Map<String,String> auditParamsMap = auditParameters.toMap();
        auditParamsMap.forEach((key, value) -> auditParamsMap.put(key, urlEncodeString(value)));
        String jsonAuditParams = mapper.writeValueAsString(auditParamsMap) + "\n";
        
        writeLock.lock();
        try {
            // if the file/stream is null, doesn't exist, or the file is too old/big, create a new file & output stream
            if (currentFile == null || !hdfs.exists(currentFile) || isFileTooOld() || isFileTooBig())
                createNewFile();
            
            writeAudit(jsonAuditParams);
        } finally {
            writeLock.unlock();
        }
    }
    
    protected String urlEncodeString(String value) {
        try {
            return URLEncoder.encode(value, "UTF8");
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to URL encode value: " + value);
        }
        return value;
    }
    
    protected void writeAudit(String jsonAuditParams) throws Exception {
        FSDataOutputStream appendStream = hdfs.append(currentFile);
        appendStream.write(jsonAuditParams.getBytes("UTF8"));
        appendStream.close();
    }
    
    protected void createNewFile() throws IOException, ParseException {
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
        FSDataOutputStream outStream = hdfs.create(currentFile);
        outStream.close();
        creationDate = currentDate;
    }
    
    protected boolean isFileTooOld() throws ParseException {
        return currentFile == null || ((new Date().getTime() - creationDate.getTime()) >= maxFileAgeMillis);
    }
    
    protected boolean isFileTooBig() throws IOException {
        return currentFile == null || hdfs.getFileStatus(currentFile).getLen() >= maxFileLenBytes;
    }
    
    public static class Builder<T extends Builder<T>> {
        protected String hdfsUri;
        protected String path;
        protected String prefix;
        protected long maxFileLenBytes;
        protected long maxFileAgeMillis;
        protected List<String> configResources;
        
        public Builder() {
            prefix = "audits";
            maxFileLenBytes = 1024L * 1024L * 512L;
            maxFileAgeMillis = TimeUnit.MINUTES.toMillis(30);
            configResources = new ArrayList<>();
        }
        
        public String getHdfsUri() {
            return hdfsUri;
        }
        
        public T setHdfsUri(String hdfsUri) {
            this.hdfsUri = hdfsUri;
            return (T) this;
        }
        
        public String getPath() {
            return path;
        }
        
        public T setPath(String path) {
            this.path = path;
            return (T) this;
        }
        
        public String getPrefix() {
            return prefix;
        }
        
        public T setPrefix(String prefix) {
            this.prefix = prefix;
            return (T) this;
        }
        
        public long getMaxFileLenBytes() {
            return maxFileLenBytes;
        }
        
        public T setMaxFileLenBytes(long maxFileLenBytes) {
            this.maxFileLenBytes = maxFileLenBytes;
            return (T) this;
        }
        
        public long getMaxFileAgeMillis() {
            return maxFileAgeMillis;
        }
        
        public T setMaxFileAgeMillis(long maxFileAgeMillis) {
            this.maxFileAgeMillis = maxFileAgeMillis;
            return (T) this;
        }
        
        public List<String> getConfigResources() {
            return configResources;
        }
        
        public T setConfigResources(List<String> configResources) {
            this.configResources = configResources;
            return (T) this;
        }
        
        public HdfsAuditor build() throws IOException, URISyntaxException {
            return new HdfsAuditor(this);
        }
    }
}
