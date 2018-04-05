package datawave.query.planner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.data.type.GeometryType;
import datawave.data.type.NumberType;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.BalancedShardPartitioner;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.config.ShardQueryConfigurationFactory;
import datawave.query.metrics.MockStatusReporter;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.Composite;
import datawave.webservice.edgedictionary.TestDatawaveEdgeDictionaryImpl;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.configuration.QueryData;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import io.protostuff.runtime.ArraySchemas;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static datawave.webservice.query.QueryParameters.QUERY_AUTHORIZATIONS;
import static datawave.webservice.query.QueryParameters.QUERY_BEGIN;
import static datawave.webservice.query.QueryParameters.QUERY_END;
import static datawave.webservice.query.QueryParameters.QUERY_EXPIRATION;
import static datawave.webservice.query.QueryParameters.QUERY_NAME;
import static datawave.webservice.query.QueryParameters.QUERY_PERSISTENCE;
import static datawave.webservice.query.QueryParameters.QUERY_STRING;

@RunWith(Arquillian.class)
public class GeoCompositeIndexTest {
    
    private static final int NUM_SHARDS = 241;
    private static final String SHARD_TABLE_NAME = "shard";
    private static final String KNOWLEDGE_SHARD_TABLE_NAME = "knowledgeShard";
    private static final String ERROR_SHARD_TABLE_NAME = "errorShard";
    private static final String SHARD_INDEX_TABLE_NAME = "shardIndex";
    private static final String SHARD_REVERSE_INDEX_TABLE_NAME = "shardReverseIndex";
    private static final String METADATA_TABLE_NAME = "DatawaveMetadata";
    private static final String DATA_TYPE_NAME = "wkt";
    private static final String INGEST_HELPER_CLASS = TestIngestHelper.class.getName();
    
    private static final String GEO_FIELD = "GEO";
    private static final String WKT_BYTE_LENGTH_FIELD = "WKT_BYTE_LENGTH";
    
    private static final String PASSWORD = "";
    private static final String AUTHS = "ALL";
    
    private static final String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(formatPattern);
    
    private static final String LEGACY_BEGIN_DATE = "20000101 000000.000";
    private static final String COMPOSITE_BEGIN_DATE = "20010101 000000.000";
    
    private static final String BEGIN_DATE = "20000101 000000.000";
    private static final String END_DATE = "20020101 000000.000";
    
    private static final String USER = "testcorp";
    private static final String USER_DN = "cn=test.testcorp.com, ou=datawave, ou=development, o=testcorp, c=us";
    
    private static final Configuration conf = new Configuration();
    
    // @formatter:off
    private static final String[] wktLegacyData = {
            "POINT(0 0)",

            "POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10))",

            "POLYGON((45 45, -45 45, -45 -45, 45 -45, 45 45))",

            "POLYGON((90 90, -90 90, -90 -90, 90 -90, 90 90))"};

    private static final Integer[] wktByteLengthLegacyData = {
            wktLegacyData[0].length(),
            wktLegacyData[1].length(),
            null,
            wktLegacyData[3].length()};

    private static final long[] legacyDates = {
            0,
            TimeUnit.DAYS.toMillis(90),
            TimeUnit.DAYS.toMillis(180),
            0};

    private static final String[] wktCompositeData = {
            "POINT(30 -85)",
            "POINT(-45 17)",

            "POLYGON((25 25, 5 25, 5 5, 25 5, 25 25))",
            "POLYGON((-20 -20, -40 -20, -40 -40, -20 -40, -20 -20))",

            "POLYGON((90 45, 0 45, 0 -45, 90 -45, 90 45))",
            "POLYGON((45 15, -45 15, -45 -60, 45 -60, 45 15))",

            "POLYGON((180 90, 0 90, 0 -90, 180 -90, 180 90))",
            "POLYGON((90 0, -90 0, -90 -180, 90 -180, 90 0))"};

    private static final Integer[] wktByteLengthCompositeData = {
            wktCompositeData[0].length(),
            wktCompositeData[1].length(),

            null,
            wktCompositeData[3].length(),

            wktCompositeData[4].length(),
            null,

            wktCompositeData[6].length(),
            wktCompositeData[7].length()};

    private static final long[] compositeDates = {
            0,
            TimeUnit.DAYS.toMillis(90),

            TimeUnit.DAYS.toMillis(180),
            0,

            TimeUnit.DAYS.toMillis(90),
            TimeUnit.DAYS.toMillis(180),

            0,
            TimeUnit.DAYS.toMillis(90)};
    // @formatter:on
    
    private static final String QUERY_WKT = "POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))";
    
    @Inject
    @SpringBean(name = "EventQuery")
    ShardQueryLogic logic;
    
    private static InMemoryInstance instance;
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "datawave.webservice.query.result.event")
                        .addClass(TestDatawaveEdgeDictionaryImpl.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @BeforeClass
    public static void setupClass() throws Exception {
        System.setProperty("subject.dn.pattern", "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)");
        
        setupConfiguration(conf);
        
        AbstractColumnBasedHandler<Text> dataTypeHandler = new AbstractColumnBasedHandler<Text>();
        dataTypeHandler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
        
        TestIngestHelper ingestHelper = new TestIngestHelper();
        ingestHelper.setup(conf);
        
        // create and process events with WKT data
        RawRecordContainer record = new RawRecordContainerImpl();
        Multimap<BulkIngestKey,Value> keyValues = HashMultimap.create();
        int recNum = 1;
        for (int dataIdx = 0; dataIdx < 2; dataIdx++) {
            
            String beginDate;
            String[] wktData;
            Integer[] wktByteLengthData;
            long[] dates;
            boolean useCompositeIngest;
            
            if (dataIdx == 0) {
                beginDate = LEGACY_BEGIN_DATE;
                wktData = wktLegacyData;
                wktByteLengthData = wktByteLengthLegacyData;
                dates = legacyDates;
                useCompositeIngest = false;
            } else {
                beginDate = COMPOSITE_BEGIN_DATE;
                wktData = wktCompositeData;
                wktByteLengthData = wktByteLengthCompositeData;
                dates = compositeDates;
                useCompositeIngest = true;
            }
            
            for (int i = 0; i < wktData.length; i++) {
                record.clear();
                record.setDataType(new Type(DATA_TYPE_NAME, TestIngestHelper.class, (Class) null, (String[]) null, 1, (String[]) null));
                record.setRawFileName("geodata_" + recNum + ".dat");
                record.setRawRecordNumber(recNum++);
                record.setDate(formatter.parse(beginDate).getTime() + dates[i]);
                record.setRawData((wktData[i] + "|" + ((wktByteLengthData[i] != null) ? Integer.toString(wktByteLengthData[i]) : "")).getBytes("UTF8"));
                record.generateId(null);
                record.setVisibility(new ColumnVisibility(AUTHS));
                
                final Multimap<String,NormalizedContentInterface> fields = ingestHelper.getEventFields(record);
                
                if (useCompositeIngest && ingestHelper instanceof CompositeIngest) {
                    Multimap<String,NormalizedContentInterface> compositeFields = ingestHelper.getCompositeFields(fields);
                    for (String fieldName : compositeFields.keySet()) {
                        // if this is an overloaded event field, we are replacing the existing data
                        if (ingestHelper.isOverloadedCompositeField(fieldName))
                            fields.removeAll(fieldName);
                        fields.putAll(fieldName, compositeFields.get(fieldName));
                    }
                }
                
                Multimap kvPairs = dataTypeHandler.processBulk(new Text(), record, fields, new MockStatusReporter());
                
                keyValues.putAll(kvPairs);
                
                dataTypeHandler.getMetadata().addEvent(ingestHelper, record, fields);
            }
        }
        keyValues.putAll(dataTypeHandler.getMetadata().getBulkMetadata());
        
        // write these values to their respective tables
        instance = new InMemoryInstance();
        Connector connector = instance.getConnector("root", PASSWORD);
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations(AUTHS));
        
        writeKeyValues(connector, keyValues);
    }
    
    public static void setupConfiguration(Configuration conf) {
        String compositeFieldName = GEO_FIELD;// + "_" + WKT_BYTE_LENGTH_FIELD;
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.COMPOSITE_FIELD_NAMES, compositeFieldName);
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.COMPOSITE_FIELD_MEMBERS, GEO_FIELD + "." + WKT_BYTE_LENGTH_FIELD);
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.COMPOSITE_FIELDS_FIXED_LENGTH, compositeFieldName);
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.COMPOSITE_FIELDS_TRANSITION_DATES, compositeFieldName + Composite.START_SEPARATOR + COMPOSITE_BEGIN_DATE);
        
        conf.set(DATA_TYPE_NAME + BaseIngestHelper.INDEX_FIELDS, GEO_FIELD + ((!compositeFieldName.equals(GEO_FIELD)) ? "," + compositeFieldName : ""));
        conf.set(DATA_TYPE_NAME + "." + GEO_FIELD + BaseIngestHelper.FIELD_TYPE, GeometryType.class.getName());
        conf.set(DATA_TYPE_NAME + "." + WKT_BYTE_LENGTH_FIELD + BaseIngestHelper.FIELD_TYPE, NumberType.class.getName());
        
        conf.set(DATA_TYPE_NAME + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE_NAME);
        conf.set(TypeRegistry.INGEST_DATA_TYPES, DATA_TYPE_NAME);
        conf.set(DATA_TYPE_NAME + TypeRegistry.INGEST_HELPER, INGEST_HELPER_CLASS);
        
        conf.set(ShardedDataTypeHandler.METADATA_TABLE_NAME, METADATA_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.NUM_SHARDS, Integer.toString(NUM_SHARDS));
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, SHARD_TABLE_NAME + "," + KNOWLEDGE_SHARD_TABLE_NAME + "," + ERROR_SHARD_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, SHARD_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_LPRIORITY, "30");
        conf.set(SHARD_TABLE_NAME + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, SHARD_INDEX_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_LPRIORITY, "30");
        conf.set(SHARD_INDEX_TABLE_NAME + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, SHARD_REVERSE_INDEX_TABLE_NAME);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_LPRIORITY, "30");
        conf.set(SHARD_REVERSE_INDEX_TABLE_NAME + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardTableConfigHelper.MARKINGS_SETUP_ITERATOR_ENABLED, "false");
        conf.set(ShardTableConfigHelper.MARKINGS_SETUP_ITERATOR_CONFIG, "");
        conf.set("partitioner.category.shardedTables", BalancedShardPartitioner.class.getName());
        conf.set("partitioner.category.member." + SHARD_TABLE_NAME, "shardedTables");
    }
    
    private static void writeKeyValues(Connector connector, Multimap<BulkIngestKey,Value> keyValues) throws Exception {
        final TableOperations tops = connector.tableOperations();
        final Set<BulkIngestKey> biKeys = keyValues.keySet();
        for (final BulkIngestKey biKey : biKeys) {
            final String tableName = biKey.getTableName().toString();
            if (!tops.exists(tableName))
                tops.create(tableName);
            
            final BatchWriter writer = connector.createBatchWriter(tableName, new BatchWriterConfig());
            for (final Value val : keyValues.get(biKey)) {
                final Mutation mutation = new Mutation(biKey.getKey().getRow());
                mutation.put(biKey.getKey().getColumnFamily(), biKey.getKey().getColumnQualifier(), biKey.getKey().getColumnVisibilityParsed(), biKey.getKey()
                                .getTimestamp(), val);
                writer.addMutation(mutation);
            }
            writer.close();
        }
    }
    
    @Test
    public void compositeWithoutIvaratorTest() throws Exception {
        // @formatter:off
        String query = "((" + GEO_FIELD + " >= '0202' && " + GEO_FIELD + " <= '020d') || " +
                "(" + GEO_FIELD + " >= '030a' && " + GEO_FIELD + " <= '0335') || " +
                "(" + GEO_FIELD + " >= '0428' && " + GEO_FIELD + " <= '0483') || " +
                "(" + GEO_FIELD + " >= '0500aa' && " + GEO_FIELD + " <= '050355') || " +
                "(" + GEO_FIELD + " >= '1f0aaaaaaaaaaaaaaa' && " + GEO_FIELD + " <= '1f36c71c71c71c71c7')) && " +
                "(" + WKT_BYTE_LENGTH_FIELD + " >= 0 && " + WKT_BYTE_LENGTH_FIELD + " < 80)";
        // @formatter:on
        
        List<QueryData> queries = getQueryRanges(query, false);
        Assert.assertEquals(12, queries.size());
        
        List<DefaultEvent> events = getQueryResults(query, false);
        
        System.out.println(queries.size() + " Queries Returned " + events.size() + " Events");
        for (DefaultEvent event : events) {
            System.out.println("\n Event: " + event.getMetadata().getInternalId());
            List<String> fieldValues = new ArrayList<>();
            for (DefaultField field : event.getFields())
                fieldValues.add("  " + field.getName() + ": " + field.getValueString());
            Collections.sort(fieldValues);
            for (String fieldValue : fieldValues)
                System.out.println(fieldValue);
        }
        
        Assert.assertEquals(9, events.size());
        
        System.out.println("done!");
    }
    
    @Test
    public void compositeWithIvaratorTest() throws Exception {
        // @formatter:off
        String query = "((" + GEO_FIELD + " >= '0202' && " + GEO_FIELD + " <= '020d') || " +
                "(" + GEO_FIELD + " >= '030a' && " + GEO_FIELD + " <= '0335') || " +
                "(" + GEO_FIELD + " >= '0428' && " + GEO_FIELD + " <= '0483') || " +
                "(" + GEO_FIELD + " >= '0500aa' && " + GEO_FIELD + " <= '050355') || " +
                "(" + GEO_FIELD + " >= '1f0aaaaaaaaaaaaaaa' && " + GEO_FIELD + " <= '1f36c71c71c71c71c7')) && " +
                "(" + WKT_BYTE_LENGTH_FIELD + " >= 0 && " + WKT_BYTE_LENGTH_FIELD + " < 80)";
        // @formatter:on
        
        List<QueryData> queries = getQueryRanges(query, true);
        Assert.assertEquals(732, queries.size());
        
        List<DefaultEvent> events = getQueryResults(query, true);
        
        System.out.println(queries.size() + " Queries Returned " + events.size() + " Events");
        for (DefaultEvent event : events) {
            System.out.println("\n Event: " + event.getMetadata().getInternalId());
            List<String> fieldValues = new ArrayList<>();
            for (DefaultField field : event.getFields())
                fieldValues.add("  " + field.getName() + ": " + field.getValueString());
            Collections.sort(fieldValues);
            for (String fieldValue : fieldValues)
                System.out.println(fieldValue);
        }
        
        Assert.assertEquals(9, events.size());
        
        System.out.println("done!");
    }
    
    private List<QueryData> getQueryRanges(String queryString, boolean useIvarator) throws Exception {
        ShardQueryLogic logic = getShardQueryLogic(useIvarator);
        
        Iterator iter = getQueryRangesIterator(queryString, logic);
        List<QueryData> queryData = new ArrayList<>();
        while (iter.hasNext())
            queryData.add((QueryData) iter.next());
        return queryData;
    }
    
    private List<DefaultEvent> getQueryResults(String queryString, boolean useIvarator) throws Exception {
        ShardQueryLogic logic = getShardQueryLogic(useIvarator);
        
        Iterator iter = getResultsIterator(queryString, logic);
        List<DefaultEvent> events = new ArrayList<>();
        while (iter.hasNext())
            events.add((DefaultEvent) iter.next());
        return events;
    }
    
    private Iterator getQueryRangesIterator(String queryString, ShardQueryLogic logic) throws Exception {
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
        params.putSingle(QUERY_STRING, queryString);
        params.putSingle(QUERY_NAME, "geoQuery");
        params.putSingle(QUERY_PERSISTENCE, "PERSISTENT");
        params.putSingle(QUERY_AUTHORIZATIONS, AUTHS);
        params.putSingle(QUERY_EXPIRATION, "20200101 000000.000");
        params.putSingle(QUERY_BEGIN, BEGIN_DATE);
        params.putSingle(QUERY_END, END_DATE);
        
        QueryParameters queryParams = new QueryParametersImpl();
        queryParams.validate(params);
        
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations(AUTHS));
        
        Query query = new QueryImpl();
        query.initialize(USER, Arrays.asList(USER_DN), null, queryParams, null);
        
        ShardQueryConfiguration config = ShardQueryConfigurationFactory.createShardQueryConfigurationFromConfiguredLogic(logic, query);
        
        logic.initialize(config, instance.getConnector("root", PASSWORD), query, auths);
        
        logic.setupQuery(config);
        
        return config.getQueries();
    }
    
    private Iterator getResultsIterator(String queryString, ShardQueryLogic logic) throws Exception {
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
        params.putSingle(QUERY_STRING, queryString);
        params.putSingle(QUERY_NAME, "geoQuery");
        params.putSingle(QUERY_PERSISTENCE, "PERSISTENT");
        params.putSingle(QUERY_AUTHORIZATIONS, AUTHS);
        params.putSingle(QUERY_EXPIRATION, "20200101 000000.000");
        params.putSingle(QUERY_BEGIN, BEGIN_DATE);
        params.putSingle(QUERY_END, END_DATE);
        
        QueryParameters queryParams = new QueryParametersImpl();
        queryParams.validate(params);
        
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations(AUTHS));
        
        Query query = new QueryImpl();
        query.initialize(USER, Arrays.asList(USER_DN), null, queryParams, null);
        
        ShardQueryConfiguration config = ShardQueryConfigurationFactory.createShardQueryConfigurationFromConfiguredLogic(logic, query);
        
        logic.initialize(config, instance.getConnector("root", PASSWORD), query, auths);
        
        logic.setupQuery(config);
        
        return logic.getTransformIterator(query);
    }
    
    private ShardQueryLogic getShardQueryLogic(boolean useIvarator) {
        ShardQueryLogic logic = new ShardQueryLogic(this.logic);
        
        // increase the depth threshold
        logic.setMaxDepthThreshold(20);
        
        // set the pushdown threshold really high to avoid collapsing uids into shards (overrides setCollapseUids if #terms is greater than this threshold)
        ((DefaultQueryPlanner) (logic.getQueryPlanner())).setPushdownThreshold(1000000);
        
        // lets avoid condensing uids to ensure that shard ranges are not collapsed into day ranges
        ((DefaultQueryPlanner) (logic.getQueryPlanner())).setCondenseUidsInRangeStream(false);
        
        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hdfsSiteConfig.toExternalForm());
        
        if (useIvarator)
            setupIvarator(logic);
        
        return logic;
    }
    
    private void setupIvarator(ShardQueryLogic logic) {
        // Set these to ensure ivarator runs
        logic.setMaxUnfieldedExpansionThreshold(1);
        logic.setMaxValueExpansionThreshold(1);
        logic.setMaxOrExpansionThreshold(1);
        logic.setMaxOrExpansionFstThreshold(1);
        logic.setIvaratorCacheScanPersistThreshold(1);
    }
    
    public static class TestIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            
            String[] values = new String(record.getRawData()).split("\\|");
            
            NormalizedContentInterface geo_nci = new NormalizedFieldAndValue(GEO_FIELD, values[0]);
            eventFields.put(GEO_FIELD, geo_nci);
            
            if (values.length > 1) {
                NormalizedContentInterface wktByteLength_nci = new NormalizedFieldAndValue(WKT_BYTE_LENGTH_FIELD, values[1]);
                eventFields.put(WKT_BYTE_LENGTH_FIELD, wktByteLength_nci);
            }
            
            return normalizeMap(eventFields);
        }
    }
}
