package datawave.core.iterators;

import datawave.query.composite.CompositeSeeker.ShardIndexCompositeSeeker;
import datawave.data.type.DiscreteIndexType;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
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
public class CompositeSeekingIterator extends WrappingIterator {

    private static final Logger log = Logger.getLogger(CompositeSeekingIterator.class);

    public static final String COMPONENT_FIELDS = "component.fields";
    public static final String DISCRETE_INDEX_TYPE = ".discrete.index.type";
    public static final String SEPARATOR = "separator";

    private List<String> fieldNames = new ArrayList<>();
    private Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexType = new HashMap<>();
    private String separator;

    private Range currentRange;
    private ShardIndexCompositeSeeker compositeSeeker;
    private Collection<ByteSequence> columnFamilies;
    private Boolean inclusive;

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CompositeSeekingIterator to = new CompositeSeekingIterator();
        to.setSource(getSource().deepCopy(env));

        Collections.copy(to.fieldNames, fieldNames);
        to.fieldToDiscreteIndexType = new HashMap<>(fieldToDiscreteIndexType);
        to.separator = separator;
        to.compositeSeeker = new ShardIndexCompositeSeeker(to.fieldNames, to.separator, to.fieldToDiscreteIndexType);

        return to;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        final String compFields = options.get(COMPONENT_FIELDS);
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

        this.separator = options.get(SEPARATOR);

        compositeSeeker = new ShardIndexCompositeSeeker(fieldNames, separator, fieldToDiscreteIndexType);
    }
    
    @Override
    public void next() throws IOException {
        super.next();
        while(hasTop() && !compositeSeeker.isKeyInRange(getTopKey(), currentRange)){
            Range newRange = compositeSeeker.nextSeekRange(getTopKey(), currentRange);
            if (newRange != currentRange) {
                currentRange = newRange;
                super.seek(currentRange, columnFamilies, inclusive);
                super.next();
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (currentRange == null)
            currentRange = range;

        if (this.columnFamilies == null)
            this.columnFamilies = columnFamilies;

        if (this.inclusive == null)
            this.inclusive = inclusive;

        super.seek(range, columnFamilies, inclusive);
    }
}
