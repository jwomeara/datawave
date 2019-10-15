package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.JexlNode;

/**
 * This will flatten ands and ors. If requested this will also remove reference expressions and references where possible. NOTE: If you remove reference
 * expressions and references, this will adversely affect the jexl evaluation of the query.
 */
public class TreeFlatteningRebuildingVisitor extends TreeFlatteningRebuildingVisitorNew {
    
    public TreeFlatteningRebuildingVisitor(boolean removeReferences) {
        super(removeReferences);
    }
    
    /**
     * This will flatten ands and ors.
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T flatten(T node) {
        return flatten(node, false);
    }
    
    /**
     * This will flatten ands, ors, and references and references expressions NOTE: If you remove reference expressions and references, this may adversely
     * affect the evaluation of the query (true in the index query logic case: bug?).
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T flattenAll(T node) {
        return flatten(node, true);
    }
    
    /**
     * This will flatten ands and ors. If requested this will also remove reference expressions and references where possible. NOTE: If you remove reference
     * expressions and references, this may adversely affect the evaluation of the query (true in the index query logic case: bug?).
     */
    @SuppressWarnings("unchecked")
    private static <T extends JexlNode> T flatten(T node, boolean removeReferences) {
        TreeFlatteningRebuildingVisitor visitor = new TreeFlatteningRebuildingVisitor(removeReferences);
        
        return (T) node.jjtAccept(visitor, null);
    }
}
