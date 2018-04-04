package datawave.query.util;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Multimap;
import datawave.data.ColumnFamilyConstants;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.security.util.ScannerHelper;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import static datawave.ingest.data.config.ingest.CompositeIngest.CONFIG_PREFIX;

/**
 */
@Configuration
@EnableCaching
@Component("compositeMetadataHelper")
public class CompositeMetadataHelper {
    private static final Logger log = Logger.getLogger(CompositeMetadataHelper.class);
    
    public static final String NULL_BYTE = "\0";

    protected final List<Text> metadataCompositeColfs = Arrays.asList(ColumnFamilyConstants.COLF_CI);
    
    protected Connector connector;
    protected Instance instance;
    protected String metadataTableName;
    protected Set<Authorizations> auths;
    
    public CompositeMetadataHelper initialize(Connector connector, String metadataTableName, Set<Authorizations> auths) {
        return this.initialize(connector, connector.getInstance(), metadataTableName, auths);
    }
    
    /**
     * Initializes the instance with a provided update interval.
     * 
     * @param connector
     *            A Connector to Accumulo
     * @param metadataTableName
     *            The name of the DatawaveMetadata table
     * @param auths
     *            Any {@link Authorizations} to use
     */
    public CompositeMetadataHelper initialize(Connector connector, Instance instance, String metadataTableName, Set<Authorizations> auths) {
        this.connector = connector;
        this.instance = instance;
        this.metadataTableName = metadataTableName;
        this.auths = auths;
        
        if (log.isTraceEnabled()) {
            log.trace("Constructor  connector: " + connector.getClass().getCanonicalName() + " with auths: " + auths + " and metadata table name: "
                            + metadataTableName);
        }
        return this;
    }
    
    public Set<Authorizations> getAuths() {
        return auths;
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    @Cacheable(value = "getCompositeMetadata", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public CompositeMetadata getCompositeMetadata() throws TableNotFoundException {
        log.debug("cache fault for getCompositeMetadata(" + this.auths + "," + this.metadataTableName + ")");
        return this.getCompositeMetadata(null);
    }
    
    @Cacheable(value = "getCompositeMetadata", key = "{#root.target.auths,#root.target.metadataTableName,#datatypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public CompositeMetadata getCompositeMetadata(Set<String> datatypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getCompositeMetadata(" + this.auths + "," + this.metadataTableName + "," + datatypeFilter + ")");
        CompositeMetadata compositeMetadata = new CompositeMetadata();
        
        // Scanner to the provided metadata table
        Scanner bs = ScannerHelper.createScanner(connector, metadataTableName, auths);
        
        Range range = new Range();
        bs.setRange(range);
        
        // Fetch all the column
        for (Text colf : metadataCompositeColfs) {
            bs.fetchColumnFamily(colf);
        }
        
        for (Entry<Key,Value> entry : bs) {
            String row = entry.getKey().getRow().toString();
            String colq = entry.getKey().getColumnQualifier().toString();
            int idx = colq.indexOf(NULL_BYTE);
            String type = colq.substring(0, idx); // this is the datatype

            if (row.startsWith(CompositeIngest.CONFIG_PREFIX)) {
                if (row.equals(CompositeIngest.TRANSITION_DATE)) {
                    if (idx != -1) {
                        String[] fieldNameAndDate = colq.substring(idx + 1).split(CompositeIngest.CONFIG_PREFIX); // this is the fieldName
                        try {
                            Date transitionDate = CompositeIngest.CompositeFieldNormalizer.formatter.parse(fieldNameAndDate[1]);
                            compositeMetadata.addTransitionDate(fieldNameAndDate[0], type, transitionDate);
                        } catch (ParseException e) {
                            log.trace("Unable to parse composite field transition date", e);
                        }
                    } else {
                        log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey().toString());
                    }
                }
            } else {
                // Get the column qualifier from the key. It contains the datatype
                // and composite name,idx
                if (null != entry.getKey().getColumnQualifier()) {
                    if (idx != -1) {
                        String field = entry.getKey().getRow().toString(); // this is the component of the composite
                        if (datatypeFilter == null || datatypeFilter.isEmpty() || datatypeFilter.contains(type)) {
                            String compositeNameAndIndex = colq.substring(idx + 1); // this is the compositename,idx
                            compositeNameAndIndex = compositeNameAndIndex.replaceAll(",", "[") + "]";
                            compositeMetadata.put(compositeNameAndIndex, type, field);
                        }
                    } else {
                        log.warn("EventMetadata entry did not contain a null byte in the column qualifier: " + entry.getKey().toString());
                    }
                } else {
                    log.warn("ColumnQualifier null in EventMetadata for key: " + entry.getKey().toString());
                }
            }
        }
        
        bs.close();
        
        return compositeMetadata;
    }

    /**
     * Invalidates all elements in all internal caches
     */
    @CacheEvict(value = {"getCompositeMetadata"}, allEntries = true, cacheManager = "metadataHelperCacheManager")
    public void evictCaches() {
        log.debug("evictCaches");
    }
}
