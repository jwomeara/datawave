package org.apache.commons.jexl2.parser;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Represents a delayed composite predicate. If this reference expression exists, we should not perform any processing that may affect the indexed query.
 */
public class ASTDelayedCompositePredicate extends ASTDelayedPredicate {

    private static final String CLASS_NAME = ASTDelayedCompositePredicate.class.getSimpleName();

    public ASTDelayedCompositePredicate(int id) {
        super(id);
    }

    public ASTDelayedCompositePredicate(Parser p, int id) {
        super(p, id);
    }

    public ASTDelayedCompositePredicate(JexlNode source) {
        super(source);
    }

    @Override
    public String toString() {
        return CLASS_NAME;
    }
    
    /**
     * @param node
     * @return
     */
    public static ASTDelayedCompositePredicate create(JexlNode node) {
        
        JexlNode parent = node.jjtGetParent();
        
        ASTDelayedCompositePredicate expr = new ASTDelayedCompositePredicate(node);
        
        if (parent != null) {
            JexlNodes.replaceChild(parent, node, expr);
        }
        
        return expr;
    }
}
