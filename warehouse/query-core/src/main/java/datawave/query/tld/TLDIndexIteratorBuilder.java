package datawave.query.tld;

import com.google.common.base.Predicate;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.iterator.builder.IndexIteratorBuilder;
import datawave.query.iterator.logic.IndexIterator;

import datawave.query.predicate.TimeFilter;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class TLDIndexIteratorBuilder extends IndexIteratorBuilder {
    @Override
    public IndexIterator newIndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, Predicate<Key> datatypeFilter, FieldIndexAggregator aggregator) {
        return new TLDIndexIterator(field, value, source, timeFilter, typeMetadata, buildDocument, datatypeFilter, aggregator,
                        createCompositePredicateFilters(field.toString()));
    }
    
}
