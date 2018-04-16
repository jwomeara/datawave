package datawave.query.iterator.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import datawave.query.attributes.PreNormalizedAttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.filter.composite.CompositePredicateFilterer;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 * This iterator is a regex ivarator that enables datatype filtering, time filtering, and field index document aggregation
 */
public class DocumentAggregatingIterator extends WrappingIterator implements DocumentIterator, CompositePredicateFilterer {
    
    protected Range seekRange;
    protected Collection<ByteSequence> seekColumnFamilies;
    boolean seekInclusive;
    protected PreNormalizedAttributeFactory attributeFactory;
    protected Key nextKey;
    protected Value nextValue;
    protected Document document;
    protected boolean buildDocument = false;
    protected final FieldIndexAggregator aggregation;
    
    public DocumentAggregatingIterator(DocumentAggregatingIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
        this.seekRange = other.seekRange;
        this.seekColumnFamilies = other.seekColumnFamilies;
        this.seekInclusive = other.seekInclusive;
        this.attributeFactory = other.attributeFactory;
        this.nextKey = other.nextKey;
        this.nextValue = other.nextValue;
        this.document = other.document;
        this.buildDocument = other.buildDocument;
        this.aggregation = other.aggregation;
    }
    
    public DocumentAggregatingIterator(boolean buildDocument, // Map<String,Set<String>> fieldToNormalizers,
                    TypeMetadata typeMetadata, FieldIndexAggregator aggregator) {
        document = new Document();
        
        // Only when this source is running over an indexOnlyField
        // do we want to add it to the Document
        this.buildDocument = buildDocument;
        if (this.buildDocument) {
            if (typeMetadata == null) {
                typeMetadata = new TypeMetadata();
            }
            // Values coming from the field index are already normalized.
            // Create specialized attributes to encapsulate this and avoid
            // double normalization
            attributeFactory = new PreNormalizedAttributeFactory(typeMetadata);
        }
        
        this.aggregation = aggregator;
    }
    
    @Override
    public void seek(Range r, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.seekRange = new Range(r);
        this.seekColumnFamilies = columnFamilies == null ? null : new ArrayList<>(columnFamilies);
        this.seekInclusive = inclusive;
        super.seek(r, columnFamilies, inclusive);
        next();
    }
    
    @Override
    public void next() throws IOException {
        if (getSource().hasTop()) {
            nextValue = getSource().getTopValue();
            // Aggregate the document. NOTE: This will advance the source iterator
            document = new Document();
            if (buildDocument) {
                nextKey = aggregation.apply(new EventToFieldIndexTransform(getSource()), document, attributeFactory);
            } else {
                nextKey = aggregation.apply(new EventToFieldIndexTransform(getSource()));
            }
        } else {
            nextKey = null;
            nextValue = null;
        }
    }
    
    @Override
    public boolean hasTop() {
        return nextKey != null;
    }
    
    @Override
    public Key getTopKey() {
        return nextKey;
    }
    
    @Override
    public Value getTopValue() {
        return nextValue;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new DocumentAggregatingIterator(this, env);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.query.iterator.IndexDocumentIterator#document()
     */
    @Override
    public Document document() {
        return document;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.query.iterator.IndexDocumentIterator#move(org.apache.accumulo.core.data.Key)
     */
    @Override
    public void move(Key pointer) throws IOException {
        seek(new Range(pointer, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive()), seekColumnFamilies, seekInclusive);
    }
    
    @Override
    public void addCompositePredicates(Set<JexlNode> compositePredicates) {
        if (getSource() instanceof CompositePredicateFilterer)
            ((CompositePredicateFilterer) getSource()).addCompositePredicates(compositePredicates);
    }
}
