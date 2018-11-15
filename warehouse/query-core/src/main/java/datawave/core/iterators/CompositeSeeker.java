package datawave.core.iterators;

import com.google.common.collect.Multimap;
import datawave.data.type.DiscreteIndexType;
import datawave.data.type.Type;
import datawave.query.composite.CompositeUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class CompositeSeeker {
    private Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexType;

    CompositeSeeker(Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexType) {
        this.fieldToDiscreteIndexType = fieldToDiscreteIndexType;
    }

    abstract public boolean isKeyInRange(Key currentKey, Range currentRange);

    abstract public Range nextSeekRange(List<String> fields, Key currentKey, Range currentRange);

    boolean isInRange(List<String> values, List<String> startValues, boolean isStartInclusive, List<String> endValues, boolean isEndInclusive) {
        for (int i = values.size(); i >= 0; i--) {
            String value = (i < values.size()) ? values.get(i) : null;
            String start = (i < startValues.size()) ? startValues.get(i) : null;
            String end = (i < endValues.size()) ? endValues.get(i) : null;

            boolean isStartValueInclusive = (i != startValues.size() - 1) || isStartInclusive;
            boolean isEndValueInclusive = (i != endValues.size() - 1) || isEndInclusive;

            // if start and end are equal, and one side is exclusive while the other is inclusive, just mark both as inclusive for our purposes
            if (start != null && end != null && isStartValueInclusive != isEndValueInclusive && start.equals(end)) {
                isStartValueInclusive = true;
                isEndValueInclusive = true;
            }

            if (value != null) {
                // only use exclusive comparison for the last value, all others are inclusive
                if (start != null && !isStartValid(value, start, isStartValueInclusive))
                    return false;

                // only use exclusive comparison for the last value, all others are inclusive
                if (end != null && !isEndValid(value, end, isEndValueInclusive))
                    return false;
            }
        }
        return true;
    }

    private boolean isStartValid(String startValue, String startBound, boolean isInclusive) {
        if (isInclusive)
            return startValue.compareTo(startBound) >= 0;
        else
            return startValue.compareTo(startBound) > 0;
    }

    private boolean isEndValid(String endValue, String endBound, boolean isInclusive) {
        if (isInclusive)
            return endValue.compareTo(endBound) <= 0;
        else
            return endValue.compareTo(endBound) < 0;
    }

    String nextLowerBound(List<String> fields, List<String> values, List<String> startValues, boolean isStartInclusive, List<String> endValues, boolean isEndInclusive) {
        String[] newValues = new String[fields.size()];

        boolean carryOver = false;
        for (int i = fields.size() - 1; i >= 0; i--) {
            DiscreteIndexType discreteIndexType = fieldToDiscreteIndexType.get(fields.get(i));
            String value = (i < values.size()) ? values.get(i) : null;
            String start = (i < startValues.size()) ? startValues.get(i) : null;
            String end = (i < endValues.size()) ? endValues.get(i) : null;

            boolean isStartValueInclusive = (i != startValues.size() - 1) || isStartInclusive;
            boolean isEndValueInclusive = (i != endValues.size() - 1) || isEndInclusive;

            // if start and end are equal, and one side is exclusive while the other is inclusive, just mark both as inclusive for our purposes
            if (start != null && end != null && isStartValueInclusive != isEndValueInclusive && start.equals(end)) {
                isStartValueInclusive = true;
                isEndValueInclusive = true;
            }

            if (value != null) {
                // if it's not fixed length, check to see if we are in range
                if (discreteIndexType == null) {
                    // value precedes start value. need to seek forward.
                    if (start != null && !isStartValid(value, start, isStartValueInclusive)) {
                        newValues[i] = start;

                        // subsequent values set to start
                        for (int j = i + 1; j < startValues.size(); j++)
                            newValues[j] = startValues.get(j);
                    }
                    // value exceeds end value. need to seek forward, and carry over.
                    else if (end != null && !isEndValid(value, end, isEndValueInclusive)) {
                        newValues[i] = start;
                        carryOver = true;

                        // subsequent values set to start
                        for (int j = i + 1; j < startValues.size(); j++)
                            newValues[j] = startValues.get(j);
                    }
                    // value is in range.
                    else {
                        newValues[i] = values.get(i);
                    }
                }
                // if it's fixed length, determine whether or not we need to increment
                else {
                    // carry over means we need to increase our value
                    if (carryOver) {
                        // value precedes start value. just seek forward and ignore previous carry over.
                        if (start != null && !isStartValid(value, start, isStartValueInclusive)) {
                            newValues[i] = start;
                            carryOver = false;

                            // subsequent values set to start
                            for (int j = i + 1; j < startValues.size(); j++)
                                newValues[j] = startValues.get(j);
                        }
                        // value is at or exceeds end value. need to seek forward, and maintain carry over.
                        else if (end != null && !isEndValid(value, end, false)) {
                            newValues[i] = start;
                            carryOver = true;

                            // subsequent values set to start
                            for (int j = i + 1; j < startValues.size(); j++)
                                newValues[j] = startValues.get(j);
                        }
                        // value is in range. just increment, and finish carry over
                        else {
                            newValues[i] = discreteIndexType.incrementIndex(values.get(i));
                            carryOver = false;
                        }
                    } else {
                        // value precedes start value. need to seek forward.
                        if (start != null && !isStartValid(value, start, isStartValueInclusive)) {
                            newValues[i] = start;

                            // subsequent values set to start
                            for (int j = i + 1; j < startValues.size(); j++)
                                newValues[j] = startValues.get(j);
                        }
                        // value exceeds end value. need to seek forward, and carry over.
                        else if (end != null && !isEndValid(value, end, isEndValueInclusive)) {
                            newValues[i] = start;
                            carryOver = true;

                            // subsequent values set to start
                            for (int j = i + 1; j < startValues.size(); j++)
                                newValues[j] = startValues.get(j);
                        }
                        // value is in range.
                        else {
                            newValues[i] = values.get(i);
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

        return builder.toString();
    }

    public static class ShardIndexCompositeSeeker extends CompositeSeeker {
        private List<String> fields;

        public ShardIndexCompositeSeeker(List<String> fields, Map<String, DiscreteIndexType<?>> fieldToDiscreteIndexType) {
            super(fieldToDiscreteIndexType);
            this.fields = fields;
        }

        @Override
        public boolean isKeyInRange(Key currentKey, Range currentRange) {
            List<String> values = Arrays.asList(currentKey.getRow().toString().split(CompositeUtils.SEPARATOR));
            List<String>  startValues = Arrays.asList(currentRange.getStartKey().getRow().toString().split(CompositeUtils.SEPARATOR));
            List<String>  endValues = Arrays.asList(currentRange.getEndKey().getRow().toString().split(CompositeUtils.SEPARATOR));
            return isInRange(values, startValues, currentRange.isStartKeyInclusive(), endValues, currentRange.isEndKeyInclusive());
        }

        public Range nextSeekRange(Key currentKey, Range currentRange) {
            return nextSeekRange(fields, currentKey, currentRange);
        }

        @Override
        public Range nextSeekRange(List<String> fields, Key currentKey, Range currentRange) {
            Key startKey = currentRange.getStartKey();
            Key endKey = currentRange.getEndKey();

            List<String> values = Arrays.asList(currentKey.getRow().toString().split(CompositeUtils.SEPARATOR));
            List<String> startValues = Arrays.asList(startKey.getRow().toString().split(CompositeUtils.SEPARATOR));
            List<String> endValues = Arrays.asList(endKey.getRow().toString().split(CompositeUtils.SEPARATOR));

            String nextLowerBound = nextLowerBound(fields, values, startValues, currentRange.isStartKeyInclusive(), endValues, currentRange.isEndKeyInclusive());

            Key newStartKey = new Key(new Text(nextLowerBound), startKey.getColumnFamily(), startKey.getColumnQualifier(), startKey.getColumnVisibility(), startKey.getTimestamp());

            // build a new range only if the new start key falls within the current range
            Range finalRange = currentRange;
            if (currentRange.contains(newStartKey))
                finalRange = new Range(newStartKey, endKey);

            return finalRange;
        }
    }

    public static class FieldIndexCompositeSeeker extends CompositeSeeker {
        private static final Logger log = Logger.getLogger(FieldIndexCompositeSeeker.class);

        public FieldIndexCompositeSeeker(Multimap<String,?> fieldDatatypes) {
            super(getFieldToDiscreteIndexTypeMap(fieldDatatypes));
        }

        @Override
        public boolean isKeyInRange(Key currentKey, Range currentRange) {
            List<String> values = Arrays.asList(currentKey.getColumnQualifier().toString().split("\0")[0].split(CompositeUtils.SEPARATOR));
            List<String> startValues = Arrays.asList(currentRange.getStartKey().getColumnQualifier().toString().split("\0")[0].split(CompositeUtils.SEPARATOR));
            List<String> endValues = Arrays.asList(currentRange.getEndKey().getColumnQualifier().toString().split("\0")[0].split(CompositeUtils.SEPARATOR));
            return isInRange(values, startValues, currentRange.isStartKeyInclusive(), endValues, currentRange.isEndKeyInclusive());
        }

        @Override
        public Range nextSeekRange(List<String> fields, Key currentKey, Range currentRange) {
            Key startKey = currentRange.getStartKey();
            Key endKey = currentRange.getEndKey();

            List<String> values = Arrays.asList(currentKey.getColumnQualifier().toString().split("\0")[0].split(CompositeUtils.SEPARATOR));
            List<String> startValues = Arrays.asList(startKey.getColumnQualifier().toString().split("\0")[0].split(CompositeUtils.SEPARATOR));
            List<String> endValues = Arrays.asList(endKey.getColumnQualifier().toString().split("\0")[0].split(CompositeUtils.SEPARATOR));

            String nextLowerBound = nextLowerBound(fields, values, startValues, currentRange.isStartKeyInclusive(), endValues, currentRange.isEndKeyInclusive());

            String startColQual = startKey.getColumnQualifier().toString();

            String newColQual = nextLowerBound + startColQual.substring(startColQual.indexOf("\0"));
            Key newStartKey = new Key(startKey.getRow(), startKey.getColumnFamily(), new Text(newColQual), startKey.getColumnVisibility(), startKey.getTimestamp());

            // build a new range only if the new start key falls within the current range
            Range finalRange = currentRange;
            if (currentRange.contains(newStartKey))
                finalRange = new Range(newStartKey, endKey);

            return finalRange;
        }

        private static Map<String,DiscreteIndexType<?>> getFieldToDiscreteIndexTypeMap(Multimap<String,?> fieldDatatypes) {
            Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexTypeMap = new HashMap<>();
            for (String field : fieldDatatypes.keySet()) {
                DiscreteIndexType discreteIndexType = null;
                for (Object typeObj : fieldDatatypes.get(field)) {
                    Type type = null;
                    if (typeObj instanceof Type) {
                        type = (Type) typeObj;
                    } else if (typeObj instanceof String) {
                        try {
                            type = Class.forName(typeObj.toString()).asSubclass(Type.class).newInstance();
                        } catch (Exception e) {
                            if (log.isTraceEnabled())
                                log.trace("Could not instantiate object for class [" + typeObj.toString() + "]");
                        }
                    }
                    if (type != null && type instanceof DiscreteIndexType && ((DiscreteIndexType) type).producesFixedLengthRanges()) {
                        if (discreteIndexType == null) {
                            discreteIndexType = (DiscreteIndexType) type;
                        } else if (!discreteIndexType.getClass().equals(type.getClass())) {
                            discreteIndexType = null;
                            break;
                        }
                    }
                }

                if (discreteIndexType != null)
                    fieldToDiscreteIndexTypeMap.put(field, discreteIndexType);
            }
            return fieldToDiscreteIndexTypeMap;
        }
    }
}
