package datawave.query.iterator.filter.composite;

import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Set;

public interface CompositePredicateFilterer {
    void addCompositePredicates(Set<JexlNode> compositePredicates);
}
