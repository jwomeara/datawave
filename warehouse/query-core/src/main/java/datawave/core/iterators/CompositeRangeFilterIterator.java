package datawave.core.iterators;

import datawave.query.util.Composite;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Filters out rows whose composite terms are outside of the range defined by the upper and lower composite bounds.
 *
 */
public class CompositeRangeFilterIterator extends Filter {
    
    private static final Logger log = Logger.getLogger(CompositeRangeFilterIterator.class);
    
    public static final String LOWER_TERM = "lower.term";
    public static final String LOWER_TERM_INCLUSIVE = "lower.term.inclusive";
    public static final String UPPER_TERM = "upper.term";
    public static final String UPPER_TERM_INCLUSIVE = "upper.term.inclusive";
    
    protected String[] lowerTerms = null;
    protected String[] upperTerms = null;
    protected boolean lowerTermInclusive = false;
    protected boolean upperTermInclusive = false;
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CompositeRangeFilterIterator to = new CompositeRangeFilterIterator();
        to.setSource(getSource().deepCopy(env));
        to.lowerTerms = lowerTerms;
        to.upperTerms = upperTerms;
        to.lowerTermInclusive = lowerTermInclusive;
        to.upperTermInclusive = upperTermInclusive;
        return to;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        final String lowerTerm = options.get(LOWER_TERM);
        if (null != lowerTerm)
            lowerTerms = lowerTerm.split(Composite.START_SEPARATOR);
        
        final String lowerTermInclusive = options.get(LOWER_TERM_INCLUSIVE);
        if (null != lowerTermInclusive)
            this.lowerTermInclusive = Boolean.parseBoolean(lowerTermInclusive);
        
        final String upperTerm = options.get(UPPER_TERM);
        if (null != upperTerm)
            upperTerms = upperTerm.split(Composite.START_SEPARATOR);
        
        final String upperTermInclusive = options.get(UPPER_TERM_INCLUSIVE);
        if (null != upperTermInclusive)
            this.upperTermInclusive = Boolean.parseBoolean(upperTermInclusive);
    }
    
    @Override
    public boolean accept(Key key, Value value) {
        String[] terms = key.getRow().toString().split(Composite.START_SEPARATOR);
        
        if (this.lowerTerms != null) {
            int numTerms = Math.min(terms.length, lowerTerms.length);
            // don't need to check the first term, as it was included in the initial range scan
            for (int i = 0; i < numTerms; i++)
                if (!((!lowerTermInclusive && terms[i].compareTo(lowerTerms[i]) > 0) || (lowerTermInclusive && terms[i].compareTo(lowerTerms[i]) >= 0)))
                    return false;
        }
        
        if (this.upperTerms != null) {
            int numTerms = Math.min(terms.length, upperTerms.length);
            // don't need to check the first term, as it was included in the initial range scan
            for (int i = 0; i < numTerms; i++)
                if (!((!upperTermInclusive && terms[i].compareTo(upperTerms[i]) > 0) || (upperTermInclusive && terms[i].compareTo(upperTerms[i]) >= 0)))
                    return false;
        }
        
        return true;
    }
}
