package datawave.microservice.audit.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.io.OutputStream;
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
public class HdfsAuditor implements Auditor, SmartLifecycle {
    
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
    protected OutputStream outStream = null;
    protected CompressionCodec codec = null;
    
    protected HdfsAuditor(Builder<?> builder) throws URISyntaxException, IOException {
        this.maxFileLenBytes = builder.maxFileLenBytes;
        this.maxFileAgeMillis = builder.maxFileAgeMillis;
        
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        for (String resource : builder.configResources)
            config.addResource(new Path(resource));
        
        hdfs = FileSystem.get(new URI(builder.hdfsUri), config);
        
        basePath = new Path(new Path(builder.hdfsUri), builder.path);
        if (!hdfs.exists(basePath))
            hdfs.mkdirs(basePath);
        
        codec = new CompressionCodecFactory(config).getCodecByClassName(builder.codecName);
        
        this.sdf = new SimpleDateFormat("'." + builder.prefix + "'-yyyyMMdd_HHmmss.SSS'.json" + ((codec != null) ? codec.getDefaultExtension() : "") + "'");
    }
    
    @Override
    public void audit(AuditParameters auditParameters) throws Exception {
        
        // convert the messages to URL-encoded JSON
        Map<String,String> auditParamsMap = auditParameters.toMap();
        auditParamsMap.forEach((key, value) -> auditParamsMap.put(key, urlEncodeString(value)));
        String jsonAuditParams = mapper.writeValueAsString(auditParamsMap) + "\n";
        
        writeLock.lock();
        try {
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
            outStream = hdfs.append().create(currentFile);
            creationDate = currentDate;
            
            if (codec != null)
                outStream = codec.createOutputStream(outStream);
        }
        return outStream;
    }
    
    protected void close() throws IOException {
        
        log.error("CLOSING HDFS FILE");
        writeLock.lock();
        
        try {
            // close the output stream, if it exists
            log.error("CLOSING OUTPUT STREAM");
            if (outStream != null) {
                outStream.close();
                outStream = null;
            }
            
            log.error("RENAMING THE FILE");
            // rename the file without the '.' prefix, if it exists
            if (currentFile != null) {
                log.error("RENAMING FILE: " + currentFile + " to " + new Path(currentFile.getParent(), currentFile.getName().substring(1)));
                hdfs.rename(currentFile, new Path(currentFile.getParent(), currentFile.getName().substring(1)));
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
    
    protected void destroy() throws Exception {
        log.error("DESTROYING AUDITOR");
        writeLock.lock();
        log.error("STILL DESTROYING AUDITOR");
        close();
    }
    
    @Override
    public boolean isAutoStartup() {
        return true;
    }
    
    @Override
    public void stop(Runnable callback) {
        try {
            destroy();
        } catch (Exception e) {
            log.error("STOP THREW AN EXCEPTION: ", e);
        }
        callback.run();
    }
    
    @Override
    public void start() {
        // no op
    }
    
    @Override
    public void stop() {
        try {
            destroy();
        } catch (Exception e) {
            log.error("STOP THREW AN EXCEPTION: ", e);
        }
    }
    
    @Override
    public boolean isRunning() {
        return false;
    }
    
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
    
    public static class Builder<T extends Builder<T>> {
        protected String hdfsUri;
        protected String path;
        protected String prefix;
        protected String codecName;
        protected long maxFileLenBytes;
        protected long maxFileAgeMillis;
        protected List<String> configResources;
        
        public Builder() {
            prefix = "audits";
            codecName = null;
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
        
        public String getCodecName() {
            return codecName;
        }
        
        public T setCodecName(String codecName) {
            this.codecName = codecName;
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
