package datawave.core.iterators;

import datawave.query.composite.CompositeUtils;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CompositeSkippingIterator extends WrappingIterator {
    
    public static final String FIXED_LENGTH_FIELDS = "fixed.fields";
    public static final String COMPOSITE_FIELDS = "composite.fields";
    
    protected List<String> fixedLengthFields = new ArrayList<>();
    protected List<String> fieldNames = new ArrayList<>();
    
    protected Range originalRange = null;
    protected List<String> startValues = new ArrayList<>();
    protected List<String> endValues = new ArrayList<>();
    
    private Collection<ByteSequence> columnFamilies;
    private boolean inclusive;
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CompositeSkippingIterator to = new CompositeSkippingIterator();
        to.setSource(getSource().deepCopy(env));
        
        Collections.copy(to.fixedLengthFields, fixedLengthFields);
        Collections.copy(to.fieldNames, fieldNames);
        
        return to;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        final String fixedFields = options.get(FIXED_LENGTH_FIELDS);
        if (fixedFields != null)
            this.fixedLengthFields = Arrays.asList(fixedFields.split(","));
        
        final String compFields = options.get(COMPOSITE_FIELDS);
        if (compFields != null)
            this.fieldNames = Arrays.asList(compFields.split(","));
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
                    boolean fixedLengthField = fixedLengthFields.contains(fieldNames.get(i));
                    String value = (i < values.length) ? values[i] : null;
                    String start = (i < startValues.size()) ? startValues.get(i) : null;
                    String end = (i < endValues.size()) ? endValues.get(i) : null;
                    
                    if (value != null) {
                        // if it's not fixed length, check to see if we are in range
                        if (!fixedLengthField) {
                            // value preceeds start value. need to seek forward.
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
                                // value preceeds start value. just seek forward and ignore previous carry over.
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
                                    newValues[i] = incrementHexRange(values[i]);
                                    carryOver = false;
                                }
                            } else {
                                // value preceeds start value. need to seek forward.
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
                
                // if the new row exceeds the row of the key, and it doesn't exceed the end row, we need to seek and call next again. otherwise, keep the
                // existing
                // row
                if (newRow.compareTo(origRow) > 0 && newRow.compareTo(originalRange.getEndKey().getRow().toString()) <= 0) {
                    Key origStartKey = originalRange.getStartKey();
                    Key startKey = new Key(new Text(newRow), origStartKey.getColumnFamily(), origStartKey.getColumnQualifier(),
                                    origStartKey.getColumnVisibility(), origStartKey.getTimestamp());
                    seek(new Range(startKey, originalRange.getEndKey()), columnFamilies, inclusive);
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
    
    private String incrementHexRange(String hexValue) {
        int length = hexValue.length();
        String format = "%0" + hexValue.length() + "x";
        if (length < 8) {
            return incrementHexRangeInteger(hexValue, format);
        } else if (length < 16) {
            return incrementHexRangeLong(hexValue, format);
        } else {
            return incrementHexRangeBigInteger(hexValue, format);
        }
    }
    
    private String incrementHexRangeInteger(String hexValue, String format) {
        return String.format(format, Integer.parseInt(hexValue, 16) + 1);
    }
    
    private String incrementHexRangeLong(String hexValue, String format) {
        return String.format(format, Long.parseLong(hexValue, 16) + 1L);
    }
    
    private String incrementHexRangeBigInteger(String hexValue, String format) {
        return String.format(format, new BigInteger(hexValue, 16).add(BigInteger.ONE));
    }
}
