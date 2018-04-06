package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTCompositePredicate;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueryPropertyMarkerVisitor extends BaseVisitor {
    
    private static final Set<String> TYPE_IDENTIFIERS;
    
    protected Set<String> typeIdentifiers = new HashSet<>();
    protected List<JexlNode> sourceNodes;
    
    private boolean identifierFound = false;

    static {
        TYPE_IDENTIFIERS = new HashSet<>();
        TYPE_IDENTIFIERS.add(IndexHoleMarkerJexlNode.class.getSimpleName());
        TYPE_IDENTIFIERS.add(ASTCompositePredicate.class.getSimpleName());
        TYPE_IDENTIFIERS.add(ASTDelayedPredicate.class.getSimpleName());
        TYPE_IDENTIFIERS.add(ExceededValueThresholdMarkerJexlNode.class.getSimpleName());
        TYPE_IDENTIFIERS.add(ExceededTermThresholdMarkerJexlNode.class.getSimpleName());
        TYPE_IDENTIFIERS.add(ExceededOrThresholdMarkerJexlNode.class.getSimpleName());
    }
    
    private QueryPropertyMarkerVisitor() {}
    
    public static boolean instanceOf(JexlNode node, Class<? extends QueryPropertyMarker> type, List<JexlNode> sourceNodes) {
        QueryPropertyMarkerVisitor visitor = new QueryPropertyMarkerVisitor();
        
        if (type != null)
            visitor.typeIdentifiers.add(type.getSimpleName());
        else
            visitor.typeIdentifiers.addAll(TYPE_IDENTIFIERS);
        
        node.jjtAccept(visitor, null);

        if (visitor.identifierFound) {
            if (sourceNodes != null)
                for (JexlNode sourceNode : visitor.sourceNodes)
                    sourceNodes.add(trimReferenceNodes(sourceNode));
            return true;
        }
        
        return false;
    }
    
    private static JexlNode trimReferenceNodes(JexlNode node) {
        if ((node instanceof ASTReference || node instanceof ASTReferenceExpression) && node.jjtGetNumChildren() == 1)
            return trimReferenceNodes(node.jjtGetChild(0));
        return node;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        if (data != null) {
            Set foundIdentifiers = (Set) data;
            
            String identifier = JexlASTHelper.getIdentifier(node);
            if (identifier != null) {
                foundIdentifiers.add(identifier);
            }
        } else {
            String identifier = JexlASTHelper.getIdentifier(node);
            // we found the identifier, but we didn't see the
            // overarching and node, let's see if we can find it
            if (identifier != null && typeIdentifiers.contains(identifier)) {
                JexlNode andParent = null;
                JexlNode curNode = node;
                JexlNode parent = node.jjtGetParent();

                // keep traversing our ancestors as long as they are either a Reference, ReferenceExpression, or AndNode
                while (parent != null && (parent instanceof ASTReference || parent instanceof ASTReferenceExpression || parent instanceof ASTAndNode)) {
                    if (parent instanceof ASTAndNode && parent.jjtGetNumChildren() > 1) {
                        andParent = parent;
                        break;
                    }
                    curNode = parent;
                }

                // if we found the overarching and node, every node other than the current
                // node (the node that leads to the marker node) is a sibling
                if (andParent != null) {
                    this.identifierFound = true;
                    List<JexlNode> siblingNodes = new ArrayList<>();
                    for (JexlNode child : JexlNodes.children(andParent)) {
                        if (!child.equals(curNode))
                            siblingNodes.add(child);
                    }
                    sourceNodes = siblingNodes;
                }
            }
        }
        return null;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (node.jjtGetNumChildren() == 1) {
            // if this is an and node with a single child, keep descending
            return node.childrenAccept(this, data);
        } else if (node.jjtGetNumChildren() > 1) {
            // if this is an and node with multiple children, and it is
            // the first one we've found, it is our potential candidate
            if (data == null) {
                List<JexlNode> siblingNodes = new ArrayList<>();
                
                // check each child to see if we found our identifier, and
                // save off the siblings as potential source nodes
                for (JexlNode child : JexlNodes.children(node)) {
                    
                    // don't look for identifiers if we already found what we were looking for
                    if (!identifierFound) {
                        Set<String> foundIdentifiers = new HashSet<>();
                        child.jjtAccept(this, foundIdentifiers);
                        
                        foundIdentifiers.retainAll(typeIdentifiers);
                        
                        // if we found our identifier, proceed to the next child node
                        if (!foundIdentifiers.isEmpty()) {
                            identifierFound = true;
                            continue;
                        }
                    }
                    
                    siblingNodes.add(child);
                }
                
                if (identifierFound)
                    sourceNodes = siblingNodes;
            }
        }
        return null;
    }
}
