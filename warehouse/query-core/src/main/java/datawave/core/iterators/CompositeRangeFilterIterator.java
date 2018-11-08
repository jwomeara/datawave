package datawave.core.iterators;

import datawave.query.composite.CompositeUtils;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.NormalizedValueArithmetic;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Filters out rows whose composite terms are outside of the range defined by the upper and lower composite bounds.
 *
 */
public class CompositeRangeFilterIterator extends Filter {
    
    private static final Logger log = Logger.getLogger(CompositeRangeFilterIterator.class);
    
    public static final String FIELD_NAME = "field.name";
    public static final String FIXED_LENGTH_FIELDS = "fixed.fields";
    public static final String COMPOSITE_FIELDS = "composite.fields";
    public static final String COMPOSITE_PREDICATE = "composite.predicate";
    public static final String COMPOSITE_TRANSITION_DATE = "composite.transition.date";
    
    // protected String fieldName = null;
    // protected List<String> fixedLengthFields = new ArrayList<>();
    protected List<String> fieldNames = new ArrayList<>();
    protected String compositePredicate = null;
    protected Script compositePredicateScript = null;
    protected Long transitionDateMillis = null;
    
    protected Range currentRange;
    protected Collection<ByteSequence> columnFamilies;
    protected boolean inclusive;
    
    protected List<Range> subRanges;
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CompositeRangeFilterIterator to = new CompositeRangeFilterIterator();
        to.setSource(getSource().deepCopy(env));
        
        // to.fieldName = fieldName;
        //
        // Collections.copy(to.fixedLengthFields, fixedLengthFields);
        
        Collections.copy(to.fieldNames, fieldNames);
        
        to.compositePredicate = compositePredicate;
        
        to.compositePredicateScript = queryToScript(to.compositePredicate);
        
        if (transitionDateMillis != null)
            to.transitionDateMillis = transitionDateMillis;
        
        return to;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        // this.fieldName = options.get(FIELD_NAME);
        //
        // final String fixedFields = options.get(FIXED_LENGTH_FIELDS);
        // if (fixedFields != null)
        // this.fixedLengthFields = Arrays.asList(fixedFields.split(","));
        
        final String compFields = options.get(COMPOSITE_FIELDS);
        if (compFields != null)
            this.fieldNames = Arrays.asList(compFields.split(","));
        
        final String compositePredicate = options.get(COMPOSITE_PREDICATE);
        if (null != compositePredicate) {
            this.compositePredicate = compositePredicate;
            this.compositePredicateScript = queryToScript(compositePredicate);
        }
        
        final String transitionDate = options.get(COMPOSITE_TRANSITION_DATE);
        if (null != transitionDate) {
            this.transitionDateMillis = Long.parseLong(transitionDate);
        }
    }
    
    // public Map<LiteralRange<?>, java.util.List<JexlNode>> expandCompositeRanges(Range range) throws Exception {
    // Map<LiteralRange<?>, java.util.List<JexlNode>> subRanges = new HashMap<>();
    //
    // String[] lowerBound = range.getStartKey().getRow().toString().split(Constants.MAX_UNICODE_STRING);
    // String[] upperBound = range.getEndKey().getRow().toString().split(Constants.MAX_UNICODE_STRING);
    //
    // boolean lowerInclusive = range.isStartKeyInclusive();
    // boolean upperInclusive = range.isEndKeyInclusive();
    //
    // java.util.List<Pair<String,String>> newRanges = new ArrayList<>();
    // int fieldIdx = 0;
    // for (String compField : this.fieldNames) {
    // String lowerComp = (lowerBound.length > fieldIdx) ? lowerBound[fieldIdx] : null;
    // String upperComp = (upperBound.length > fieldIdx) ? upperBound[fieldIdx] : null;
    //
    // if (fixedLengthFields.contains(compField) && lowerComp != null && upperComp != null) {
    // java.util.List<String> values = expandRange(lowerComp, upperComp);
    //
    // if (newRanges.size() > 0) {
    // newRanges = newRanges.stream().flatMap(x -> buildRanges(x, values).stream()).collect(Collectors.toList());
    // } else {
    // newRanges = values.stream().map(x -> new Pair<>(x, x)).collect(Collectors.toList());
    // }
    // fieldIdx++;
    // } else {
    // StringBuilder lowerBoundTail = new StringBuilder();
    // for (int i = fieldIdx; i < lowerBound.length; i++)
    // lowerBoundTail.append(Constants.MAX_UNICODE_STRING).append(lowerBound[i]);
    // StringBuilder upperBoundTail = new StringBuilder();
    // for (int i = fieldIdx; i < upperBound.length; i++)
    // upperBoundTail.append(Constants.MAX_UNICODE_STRING).append(upperBound[i]);
    //
    // newRanges = newRanges.stream()
    // .map(x -> new Pair<>(x.getValue0() + lowerBoundTail.toString(), x.getValue1() + upperBoundTail.toString()))
    // .collect(Collectors.toList());
    // break;
    // }
    // }
    // newRanges.forEach(x -> subRanges.put(new LiteralRange<String>(x.getValue0(), lowerInclusive, x.getValue1(), upperInclusive, fieldName,
    // operand), buildRangeNodes(x.getValue0(), lowerInclusive, x.getValue1(), upperInclusive, fieldName)));
    //
    // return subRanges;
    // }
    //
    // // TODO: Change this back so it works for all types, not just hex
    // private java.util.List<String> expandRange(String lower, String upper) {
    // java.util.List<String> values = new ArrayList<>();
    // for (String value = lower; value.compareTo(upper) <= 0; value = CompositeUtils.incrementBound(value)) {
    // char lastChar = value.charAt(value.length() - 1);
    // if ((lastChar >= 'a' && lastChar <= 'f') || (lastChar >= '0' && lastChar <= '9'))
    // values.add(value);
    // }
    // return values;
    // }
    //
    // private List<Pair<String,String>> buildRanges(Pair<String,String> srcRange, List<String> values) {
    // List<Pair<String,String>> newRanges = new ArrayList<>();
    // for (String value : values)
    // newRanges.add(new Pair<>(srcRange.getValue0() + Constants.MAX_UNICODE_STRING + value, srcRange.getValue1() + Constants.MAX_UNICODE_STRING + value));
    // return newRanges;
    // }
    //
    // private List<JexlNode> buildRangeNodes(String lower, boolean lowerInclusive, String upper, boolean upperInclusive, String fieldName) {
    // List<JexlNode> nodes = new ArrayList<>();
    // nodes.add((lowerInclusive ? JexlNodeFactory.buildNode((ASTGENode) null, fieldName, lower) : JexlNodeFactory.buildNode((ASTGTNode) null, fieldName,
    // lower)));
    // nodes.add((upperInclusive ? JexlNodeFactory.buildNode((ASTLENode) null, fieldName, upper) : JexlNodeFactory.buildNode((ASTLTNode) null, fieldName,
    // upper)));
    // return nodes;
    // }
    
    private Script queryToScript(String query) {
        JexlEngine engine = new JexlEngine(null, new NormalizedValueArithmetic(), null, null);
        engine.setCache(1024);
        engine.setSilent(false);
        
        // Setting strict to be true causes an Exception when a field
        // in the query does not occur in the document being tested.
        // This doesn't appear to have any unexpected consequences looking
        // at the Interpreter class in JEXL.
        engine.setStrict(false);
        
        return engine.createScript(query);
    }
    
    // @Override
    // public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // this.currentRange = range;
    // this.columnFamilies = columnFamilies;
    // this.inclusive = inclusive;
    // super.seek(range, columnFamilies, inclusive);
    // }
    
    // when next is called, we need to decide whether or not we need to seek to the next sub-range
    // @Override
    // public void next() throws IOException {
    // super.next();
    //
    // SortedKeyValueIterator<Key,Value> source = getSource();
    // if (source.hasTop()) {
    // Key topKey = source.getTopKey();
    //
    // // if the top key exceeds the current subrange, find the first subrange which starts AFTER this key
    // Range curSubRange = subRanges.get(0);
    //
    // // if our top key exceeds the current sub-range, we need to seek to the next valid sub-range
    // if (curSubRange.afterEndKey(topKey)) {
    // subRanges.removeAt(0);
    // Iterator<Range> itr = subRanges.iterator();
    // while(itr.hasNext()) {
    // Range subRange = itr.next();
    //
    // if (!subRange.beforeStartKey(topKey)) {
    // itr.remove();
    // }
    // }
    // }
    // Iterator<Range> itr = subRanges.iterator();
    // while(itr.hasNext()) {
    // Range subRange = itr.next();
    //
    // if (subRange.afterEndKey(topKey)) {
    // itr.remove();
    // }
    // }
    // }
    // }
    
    @Override
    public boolean accept(Key key, Value value) {
        String[] terms = key.getRow().toString().split(CompositeUtils.SEPARATOR);
        MapContext jexlContext = new MapContext();
        
        if (terms.length == fieldNames.size()) {
            for (int i = 0; i < fieldNames.size(); i++)
                jexlContext.set(fieldNames.get(i), terms[i]);
            
            if (!ArithmeticJexlEngines.isMatched(compositePredicateScript.execute(jexlContext))) {
                if (log.isTraceEnabled())
                    log.trace("Filtered out an entry using the composite range filter iterator: " + jexlContext);
                return false;
            }
            
            return true;
        } else if (terms.length == 1 && transitionDateMillis != null && key.getTimestamp() < transitionDateMillis) {
            return true;
        } else {
            return false;
        }
    }
    
    private boolean something() {
        return false;
    }
    
    private Range nextRange() {
        return null;
    }
}
