package datawave.core.iterators;

import datawave.data.type.DiscreteIndexType;
import datawave.query.composite.CompositeUtils;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Skips rows whose composite terms are outside of the range defined by the upper and lower composite bounds.
 *
 */
public class CompositeSkippingIterator extends WrappingIterator {

    private static final Logger log = Logger.getLogger(CompositeSkippingIterator.class);

    public static final String COMPOSITE_FIELDS = "composite.fields";
    public static final String DISCRETE_INDEX_TYPE = ".discrete.index.type";
    
    protected List<String> fieldNames = new ArrayList<>();
    protected Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexType = new HashMap<>();
    
    protected Range originalRange = null;
    protected List<String> startValues = new ArrayList<>();
    protected List<String> endValues = new ArrayList<>();
    
    private Collection<ByteSequence> columnFamilies;
    private boolean inclusive;
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CompositeSkippingIterator to = new CompositeSkippingIterator();
        to.setSource(getSource().deepCopy(env));
        
        Collections.copy(to.fieldNames, fieldNames);
        to.fieldToDiscreteIndexType = new HashMap<>(fieldToDiscreteIndexType);

        return to;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        final String compFields = options.get(COMPOSITE_FIELDS);
        if (compFields != null)
            this.fieldNames = Arrays.asList(compFields.split(","));

        for (String fieldName : fieldNames) {
            DiscreteIndexType type = null;
            String typeClass = options.get(fieldName + DISCRETE_INDEX_TYPE);
            if (typeClass != null) {
                try {
                    type = Class.forName(typeClass).asSubclass(DiscreteIndexType.class).newInstance();
                } catch (Exception e) {
                    log.warn("Unable to create DiscreteIndexType for class name: " + typeClass);
                }
            }

            if (type != null)
                fieldToDiscreteIndexType.put(fieldName, type);
        }
    }
    
    @Override
    public void next() throws IOException {
        boolean keepGoing = true;
        while (keepGoing) {
            super.next();
            
            if (hasTop()) {
                String origRow = getTopKey().getRow().toString();
                String[] values = origRow.split(CompositeUtils.SEPARATOR);
                String[] newValues = new String[fieldNames.size()];
                
                boolean carryOver = false;
                for (int i = fieldNames.size() - 1; i >= 0; i--) {
                    DiscreteIndexType discreteIndexType = fieldToDiscreteIndexType.get(fieldNames.get(i));
                    String value = (i < values.length) ? values[i] : null;
                    String start = (i < startValues.size()) ? startValues.get(i) : null;
                    String end = (i < endValues.size()) ? endValues.get(i) : null;
                    
                    if (value != null) {
                        // if it's not fixed length, check to see if we are in range
                        if (discreteIndexType == null) {
                            // value precedes start value. need to seek forward.
                            if (start != null && value.compareTo(start) < 0) {
                                newValues[i] = start;

                                // subsequent values set to start
                                for (int j = i + 1; j < newValues.length; j++)
                                    newValues[j] = startValues.get(j);
                            }
                            // value exceeds end value. need to seek forward, and carry over.
                            else if (end != null && value.compareTo(end) > 0) {
                                newValues[i] = start;
                                carryOver = true;

                                // subsequent values set to start
                                for (int j = i + 1; j < newValues.length; j++)
                                    newValues[j] = startValues.get(j);
                            }
                            // value is in range.
                            else {
                                newValues[i] = values[i];
                            }
                        }
                        // if it's fixed length, determine whether or not we need to increment
                        else {
                            // carry over means we need to increase our value
                            if (carryOver) {
                                // value precedes start value. just seek forward and ignore previous carry over.
                                if (start != null && value.compareTo(start) < 0) {
                                    newValues[i] = start;
                                    carryOver = false;
                                    
                                    // subsequent values set to start
                                    for (int j = i + 1; j < startValues.size(); j++)
                                        newValues[j] = startValues.get(j);
                                }
                                // value is at or exceeds end value. need to seek forward, and maintain carry over.
                                else if (end != null && value.compareTo(end) >= 0) {
                                    newValues[i] = start;
                                    carryOver = true;
                                    
                                    // subsequent values set to start
                                    for (int j = i + 1; j < startValues.size(); j++)
                                        newValues[j] = startValues.get(j);
                                }
                                // value is in range. just increment, and finish carry over
                                else {
                                    newValues[i] = discreteIndexType.incrementIndex(values[i]);
                                    carryOver = false;
                                }
                            } else {
                                // value precedes start value. need to seek forward.
                                if (start != null && value.compareTo(start) < 0) {
                                    newValues[i] = start;
                                    
                                    // subsequent values set to start
                                    for (int j = i + 1; j < startValues.size(); j++)
                                        newValues[j] = startValues.get(j);
                                }
                                // value exceeds end value. need to seek forward, and carry over.
                                else if (end != null && value.compareTo(end) > 0) {
                                    newValues[i] = start;
                                    carryOver = true;
                                    
                                    // subsequent values set to start
                                    for (int j = i + 1; j < startValues.size(); j++)
                                        newValues[j] = startValues.get(j);
                                }
                                // value is in range.
                                else {
                                    newValues[i] = values[i];
                                }
                            }
                        }
                    }
                }
                
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < newValues.length; i++) {
                    if (newValues[i] != null)
                        if (i > 0)
                            builder.append(CompositeUtils.SEPARATOR).append(newValues[i]);
                        else
                            builder.append(newValues[i]);
                    else
                        break;
                }
                
                String newRow = builder.toString();
                
                // if the new row exceeds the original row of the key, and it doesn't exceed the end row, we need to seek and call next again. otherwise, keep the original row
                if (newRow.compareTo(origRow) > 0 && newRow.compareTo(originalRange.getEndKey().getRow().toString()) <= 0) {
                    Key origStartKey = originalRange.getStartKey();
                    Key startKey = new Key(new Text(newRow), origStartKey.getColumnFamily(), origStartKey.getColumnQualifier(),
                                    origStartKey.getColumnVisibility(), origStartKey.getTimestamp());
                    seek(new Range(startKey, originalRange.isStartKeyInclusive(), originalRange.getEndKey(), originalRange.isEndKeyInclusive()), columnFamilies, inclusive);
                    continue;
                }
            }
            keepGoing = false;
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (originalRange == null) {
            originalRange = range;
            
            startValues = Arrays.asList(range.getStartKey().getRow().toString().split(CompositeUtils.SEPARATOR));
            endValues = Arrays.asList(range.getEndKey().getRow().toString().split(CompositeUtils.SEPARATOR));
            
            this.columnFamilies = columnFamilies;
            this.inclusive = inclusive;
        }
        super.seek(range, columnFamilies, inclusive);
    }
}
