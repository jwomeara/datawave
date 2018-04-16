package datawave.query.iterator.builder;

import org.apache.hadoop.io.Text;

import datawave.query.iterator.logic.IndexIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;

public class CardinalityIteratorBuilder extends IndexIteratorBuilder {
    
    @SuppressWarnings("unchecked")
    public IndexIteratorBridge build() {
        if (notNull(field, value, source, datatypeFilter, keyTform, timeFilter)) {
            IndexIteratorBridge itr = new IndexIteratorBridge(new IndexIterator(new Text(field), new Text(value), source, this.timeFilter, this.typeMetadata,
                            this.fieldsToAggregate == null ? false : this.fieldsToAggregate.contains(field), this.datatypeFilter, this.keyTform,
                            createCompositePredicateFilters(field)));
            field = null;
            value = null;
            source = null;
            timeFilter = null;
            datatypeFilter = null;
            keyTform = null;
            return itr;
        } else {
            StringBuilder msg = new StringBuilder(256);
            msg.append("Cannot build iterator-- a field was null!\n");
            if (field == null) {
                msg.append("\tField was null!\n");
            }
            if (value == null) {
                msg.append("\tValue was null!\n");
            }
            if (source == null) {
                msg.append("\tSource was null!\n");
            }
            msg.setLength(msg.length() - 1);
            throw new IllegalStateException(msg.toString());
        }
    }
    
}
