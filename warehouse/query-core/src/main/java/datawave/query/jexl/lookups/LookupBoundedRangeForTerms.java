package datawave.query.jexl.lookups;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import datawave.core.iterators.ColumnQualifierRangeIterator;
import datawave.core.iterators.CompositeSkippingIterator;
import datawave.core.iterators.TimeoutExceptionIterator;
import datawave.core.iterators.TimeoutIterator;
import datawave.data.type.DiscreteIndexType;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.IllegalRangeArgumentException;
import datawave.query.jexl.LiteralRange;
import datawave.query.tables.ScannerFactory;
import datawave.util.time.DateHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class LookupBoundedRangeForTerms extends IndexLookup {
    private static final Logger log = ThreadConfigurableLogger.getLogger(LookupBoundedRangeForTerms.class);
    
    protected Set<String> datatypeFilter;
    protected Set<Text> fields;
    private final List<LiteralRange<?>> literalRanges;
    protected String fieldName;

    public LookupBoundedRangeForTerms(LiteralRange<?> literalRange) {
        this(Collections.singletonList(literalRange));
    }
    
    public LookupBoundedRangeForTerms(List<LiteralRange<?>> literalRanges) {
        this.literalRanges = literalRanges;
        datatypeFilter = Sets.newHashSet();
        fields = Sets.newHashSet();
        init();
    }
    
    private void init() {
        List<String> fieldNames = literalRanges.stream().map(LiteralRange::getFieldName).distinct().collect(Collectors.toList());
        if (fieldNames.size() != 1) {
            QueryException qe = new QueryException(DatawaveErrorCode.RANGE_CREATE_ERROR, "Field name mismatch for batched ranges.");
            log.debug(qe);
            throw new IllegalRangeArgumentException(qe);
        } else {
            this.fieldName = fieldNames.get(0);
        }
    }
    
    @Override
    public IndexLookupMap lookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, long maxLookup) {
        String startDay = DateHelper.format(config.getBeginDate());
        String endDay = DateHelper.format(config.getEndDate());
        
        // build the start and end range for the scanner
        // Key for global index is Row-> Normalized FieldValue, CF-> FieldName,
        // CQ->shard_id\x00datatype
        IndexLookupMap fieldToUniqueTerms = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
        
        IteratorSetting fairnessIterator = null;
        if (maxLookup > 0) {
            /**
             * The fairness iterator solves the problem whereby we have runaway iterators as a result of an evaluation that never finds anything
             */
            fairnessIterator = new IteratorSetting(1, TimeoutIterator.class);
            
            long maxTime = maxLookup;
            if (maxTime < Long.MAX_VALUE / 2)
                maxTime *= 2;
            fairnessIterator.addOption(TimeoutIterator.MAX_SESSION_TIME, Long.valueOf(maxTime).toString());
            
        }
        
        List<Range> ranges = new ArrayList<>();
        Key minKey = null, maxKey = null;
        for (LiteralRange<?> literalRange : literalRanges) {
            String lower = literalRange.getLower().toString(), upper = literalRange.getUpper().toString();
            
            fields.add(new Text(literalRange.getFieldName()));
            Key startKey;
            if (literalRange.isLowerInclusive()) { // inclusive
                startKey = new Key(new Text(lower));
            } else { // non-inclusive
                startKey = new Key(new Text(lower + "\0"));
            }
            
            Key endKey;
            if (literalRange.isUpperInclusive()) {
                // we should have our end key be the end of the range if we are going to use the WRI
                endKey = new Key(new Text(upper), new Text(literalRange.getFieldName()), new Text(endDay + Constants.MAX_UNICODE_STRING));
            } else {
                endKey = new Key(new Text(upper));
            }
            
            if (minKey == null || startKey.compareTo(minKey) < 0)
                minKey = startKey;
            
            if (maxKey == null || endKey.compareTo(maxKey) > 0)
                maxKey = endKey;
            
            Range range = null;
            try {
                range = new Range(startKey, true, endKey, literalRange.isUpperInclusive());
            } catch (IllegalArgumentException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.RANGE_CREATE_ERROR, e, MessageFormat.format("{0}", literalRange));
                log.debug(qe);
                throw new IllegalRangeArgumentException(qe);
            }
            
            ranges.add(range);
        }
        
        if (ranges.size() == 1)
            log.debug("Range: " + ranges.get(0));
        else
            log.debug("Batched Range: " + new Range(minKey, maxKey));
        
        BatchScanner bs = null;
        try {
            bs = scannerFactory.newScanner(config.getIndexTableName(), config.getAuthorizations(), config.getNumQueryThreads(), config.getQuery());
            
            if (log.isDebugEnabled()) {
                if (ranges.size() == 1)
                    log.debug("Range: " + ranges.get(0));
                else
                    log.debug("Batched Range: " + new Range(minKey, maxKey));
            }
            
            bs.setRanges(ranges);
            bs.fetchColumnFamily(new Text(fieldName));
            
            if (config.getDatatypeFilter() != null && !config.getDatatypeFilter().isEmpty()) {
                datatypeFilter.addAll(config.getDatatypeFilter());
            }
            
            // set up the GlobalIndexRangeSamplingIterator
            
            IteratorSetting cfg = new IteratorSetting(config.getBaseIteratorPriority() + 50, "WholeRowIterator", WholeRowIterator.class);
            bs.addScanIterator(cfg);
            
            cfg = new IteratorSetting(config.getBaseIteratorPriority() + 48, "DateFilter", ColumnQualifierRangeIterator.class);
            // search from 20YYddMM to 20ZZddMM\uffff to ensure we encompass all of the current day
            String end = endDay + Constants.MAX_UNICODE_STRING;
            cfg.addOption(ColumnQualifierRangeIterator.RANGE_NAME, ColumnQualifierRangeIterator.encodeRange(new Range(startDay, end)));
            
            bs.addScanIterator(cfg);

            // TODO: Make sure this is a composite range, and not just an expanded (overloaded) base term before adding the iterator
            // If this is a composite range, we need to setup our query to filter based on each component of the composite range
            if (config.getCompositeToFieldMap().get(fieldName) != null) {
                Date transitionDate = null;
                // if (config.getCompositeTransitionDates().containsKey(fieldName))
                // transitionDate = config.getCompositeTransitionDates().get(fieldName);
                
                // if this is a transitioned composite field, don't add the iterator if our date range preceeds the transition date
                // if (transitionDate == null || config.getEndDate().compareTo(transitionDate) > 0) {
                
                IteratorSetting compositeIterator = new IteratorSetting(config.getBaseIteratorPriority() + 51, CompositeSkippingIterator.class);
                
                compositeIterator.addOption(CompositeSkippingIterator.COMPOSITE_FIELDS,
                                StringUtils.collectionToCommaDelimitedString(config.getCompositeToFieldMap().get(fieldName)));

                for (String fieldName : config.getCompositeToFieldMap().get(fieldName)) {
                    DiscreteIndexType type = config.getFieldToDiscreteIndexTypes().get(fieldName);
                    if (type != null)
                        compositeIterator.addOption(fieldName + CompositeSkippingIterator.DISCRETE_INDEX_TYPE, type.getClass().getName());
                }

                // compositeIterator.addOption(CompositeSkippingIterator.COMPOSITE_PREDICATE, JexlStringBuildingVisitor.buildQuery(compositePredicate));
                //
                // if (transitionDate != null) {
                // // Ensure iterator runs before wholerowiterator so we get valid timestamps
                // compositeIterator.setPriority(config.getBaseIteratorPriority() + 49);
                // compositeIterator.addOption(CompositeSkippingIterator.COMPOSITE_TRANSITION_DATE, Long.toString(transitionDate.getTime()));
                // }
                
                bs.addScanIterator(compositeIterator);
                // }
            }
            
            if (null != fairnessIterator) {
                cfg = new IteratorSetting(config.getBaseIteratorPriority() + 100, TimeoutExceptionIterator.class);
                bs.addScanIterator(cfg);
            }
            
            try {
                timedScan(bs.iterator(), fieldToUniqueTerms, config, datatypeFilter, fields, false, maxLookup, null);
            } finally {
                scannerFactory.close(bs);
            }
            
        } catch (TableNotFoundException e) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.TABLE_NOT_FOUND, e, MessageFormat.format("Table: {0}",
                            config.getIndexTableName()));
            log.error(qe);
            throw new DatawaveFatalQueryException(qe);
            
        } catch (IOException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.RANGE_CREATE_ERROR, e);
            log.debug(qe);
            throw new IllegalRangeArgumentException(qe);
        } finally {
            if (bs != null)
                scannerFactory.close(bs);
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Found " + fieldToUniqueTerms.size() + " matching terms for range: " + fieldToUniqueTerms.toString());
        }
        
        return fieldToUniqueTerms;
    }
    
    protected Callable<Boolean> createTimedCallable(final Iterator<Entry<Key,Value>> iter, final IndexLookupMap fieldsToValues, ShardQueryConfiguration config,
                    Set<String> datatypeFilter, final Set<Text> fields, boolean isReverse, long timeout) {
        final Set<String> myDatatypeFilter = datatypeFilter;
        return () -> {
            Text holder = new Text();
            try {
                if (log.isTraceEnabled()) {
                    log.trace("Do we have next? " + iter.hasNext());
                    
                }
                while (iter.hasNext()) {
                    
                    Entry<Key,Value> entry = iter.next();
                    
                    if (TimeoutExceptionIterator.exceededTimedValue(entry)) {
                        throw new Exception("Timeout exceeded for bounded range lookup");
                    }
                    Key k = entry.getKey();
                    
                    if (log.isTraceEnabled()) {
                        log.trace("Foward Index entry: " + entry.getKey().toString());
                    }
                    
                    k.getRow(holder);
                    String uniqueTerm = holder.toString();
                    
                    SortedMap<Key,Value> keymap = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
                    
                    String field = null;
                    
                    boolean foundDataType = false;
                    
                    for (Key topKey : keymap.keySet()) {
                        if (null == field) {
                            topKey.getColumnFamily(holder);
                            field = holder.toString();
                        }
                        // Get the column qualifier from the key. It
                        // contains the datatype and normalizer class
                        
                        if (null != topKey.getColumnQualifier()) {
                            if (null != myDatatypeFilter && myDatatypeFilter.size() > 0) {
                                
                                String colq = topKey.getColumnQualifier().toString();
                                int idx = colq.indexOf(Constants.NULL);
                                
                                if (idx != -1) {
                                    String type = colq.substring(idx + 1);
                                    
                                    // If types are specified and this type
                                    // is not in the list, skip it.
                                    if (myDatatypeFilter.contains(type)) {
                                        
                                        if (log.isTraceEnabled())
                                            log.trace(myDatatypeFilter + " contains " + type);
                                        foundDataType = true;
                                        break;
                                    }
                                    
                                }
                            } else
                                foundDataType = true;
                        }
                    }
                    if (foundDataType) {
                        
                        // obtaining the size of a map can be expensive,
                        // instead
                        // track the count of each unique item added.
                        fieldsToValues.put(field, uniqueTerm);
                        
                        // safety check...
                        Preconditions.checkState(field.equals(fieldName), "Got an unexpected field name when expanding range" + field + " " + fieldName);
                        
                        // If this range expands into to many values, we can
                        // stop
                        if (fieldsToValues.get(field).isThresholdExceeded()) {
                            return true;
                            
                        }
                    }
                }
                
            } catch (Exception e) {
                log.info("Failed or timed out expanding range fields: " + e.getMessage());
                if (log.isDebugEnabled())
                    log.debug("Failed or Timed out ", e);
                if (fields.size() >= 1) {
                    for (Text fieldTxt : fields) {
                        String field = fieldTxt.toString();
                        if (log.isTraceEnabled()) {
                            log.trace("field is " + field);
                            log.trace("field is " + (null == fieldsToValues));
                        }
                        fieldsToValues.put(field, "");
                        fieldsToValues.get(field).setThresholdExceeded();
                    }
                } else
                    fieldsToValues.setKeyThresholdExceeded();
                return false;
            }
            
            return true;
        };
    }
    
}
