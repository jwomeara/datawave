package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.newInstanceOfType;

/**
 * This will flatten ands and ors. If requested this will also remove reference expressions and references where possible. NOTE: If you remove reference
 * expressions and references, this will adversely affect the jexl evaluation of the query.
 */
public class TreeFlatteningRebuildingVisitorNew extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(TreeFlatteningRebuildingVisitorNew.class);
    private boolean removeReferences = false;

    private long newNodesCreated = 0l;
    
    public TreeFlatteningRebuildingVisitorNew(boolean removeReferences) {
        this.removeReferences = removeReferences;
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
    private static <T extends JexlNode> T flatten(T rootNode, boolean removeReferences) {
        TreeFlatteningRebuildingVisitorNew visitor = new TreeFlatteningRebuildingVisitorNew(removeReferences);

        // add all the nodes to the stack and iterate...
        Deque<JexlNode> postOrderStack = new LinkedList<>();
        Deque<JexlNode> workingStack = new LinkedList<>();
        workingStack.push(rootNode);

        // first, compute the post order traversal of all of the nodes.
        while (!workingStack.isEmpty()) {
            JexlNode node = workingStack.pop();

            postOrderStack.push(node);

            if (node.jjtGetNumChildren() > 0) {
                for (JexlNode child : children(node)){
                    workingStack.push(child);
                }
            }
        }

        JexlNode newRoot = null;

        Deque<JexlNode> parentStack = new LinkedList<>();
        Deque<List<JexlNode>> childrenStack = new LinkedList<>();

        // now that we have the post order traversal, we can operate on the nodes...
        while (!postOrderStack.isEmpty()) {
            JexlNode node = postOrderStack.pop();

            System.out.println("Nodes left: " + postOrderStack.size() + ", Parent Stack: " + parentStack.size());

            if (node.equals(parentStack.peek())) {
                parentStack.pop();
                List<JexlNode> children = childrenStack.pop();

                System.out.println("Creating a new node in flatten.  children: " + children.size());
                visitor.newNodesCreated++;

                JexlNode newParent = JexlNodes.newInstanceOfType(node);
                newParent.image = node.image;

                int nodeIdx = 0;
                for (JexlNode child : children) {
                    child.jjtSetParent(newParent);
                    newParent.jjtAddChild(child, nodeIdx++);
                }
                children.clear();

                newParent.jjtSetParent(node.jjtGetParent());

                node = newParent;
            }

            List<JexlNode> children = new ArrayList<>();
            if (node instanceof ASTReference) {
                children.add((JexlNode) visitor.visit((ASTReference) node, null));
            } else if (node instanceof ASTReferenceExpression) {
                children.add((JexlNode) visitor.visit((ASTReferenceExpression) node, null));
            } else if (node instanceof ASTOrNode) {
                children.add(visitor.flattenTree(node, null));
            } else if (node instanceof ASTAndNode) {
                children.add(visitor.flattenTree(node, null));
            } else {
                children.add((JexlNode) node.jjtAccept(visitor, null));
            }

            if (node.jjtGetParent() != null) {
                if (!node.jjtGetParent().equals(parentStack.peek())) {
                    parentStack.push(node.jjtGetParent());
                    childrenStack.push(children);
                } else {
                    childrenStack.peek().addAll(children);
                }
            } else {
                newRoot = children.get(0);
            }
        }

        System.out.println("New nodes created: " + visitor.newNodesCreated);

        return (T) newRoot;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
//        ASTOrNode orNode = JexlNodes.newInstanceOfType(node);
//        orNode.image = node.image;
        
        return flattenTree(node, data);
    }
    
    private boolean isBoundedRange(ASTAndNode node) {
        List<JexlNode> otherNodes = new ArrayList<>();
        Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic(node, otherNodes, false);
        if (ranges.size() == 1 && otherNodes.isEmpty()) {
            return true;
        }
        return false;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
//        if (QueryPropertyMarker.instanceOf(node, null)) {
//            JexlNode rebuiltNode = (JexlNode) super.visit(node, data);
//            return rebuiltNode;
//        } else {
//            ASTAndNode andNode = JexlNodes.newInstanceOfType(node);
//            andNode.image = node.image;
//
            return flattenTree(node, data);
//        }
        
    }
    
     @Override
     public Object visit(ASTReference node, Object data) {
        return visitRef(node, data);
     }
    
     @Override
     public Object visit(ASTReferenceExpression node, Object data) {
        return visitRef(node, data);
     }
    
//    @Override
//    public Object visit(ASTReference node, Object data) {
//        if (!removeReferences) {
//            return node;
//        } else if (ASTDelayedPredicate.instanceOf(node) || IndexHoleMarkerJexlNode.instanceOf(node) || ASTEvaluationOnly.instanceOf(node)) {
//            return node;
//        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(node) || ExceededTermThresholdMarkerJexlNode.instanceOf(node)
//                        || ExceededOrThresholdMarkerJexlNode.instanceOf(node)) {
//            return node;
//        } else if (JexlASTHelper.dereference(node) instanceof ASTAssignment) {
//            return node;
//        } else if (node.jjtGetParent() instanceof ASTAndNode) {
//            ASTAndNode andNode = JexlNodes.newInstanceOfType(((ASTAndNode) (node.jjtGetParent())));
//            andNode.image = node.jjtGetParent().image;
//
//            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//                JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
//                newNode.jjtSetParent(andNode);
//                andNode.jjtAddChild(newNode, andNode.jjtGetNumChildren());
//            }
//
//            return andNode;
//        } else if (node.jjtGetParent() instanceof ASTOrNode) {
//            ASTOrNode orNode = JexlNodes.newInstanceOfType(((ASTOrNode) (node.jjtGetParent())));
//            orNode.image = node.jjtGetParent().image;
//
//            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//                JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
//                newNode.jjtSetParent(orNode);
//                orNode.jjtAddChild(newNode, orNode.jjtGetNumChildren());
//            }
//
//            return orNode;
//
//        } else if (node.jjtGetParent() instanceof ASTNotNode) {
//            // ensure we keep negated expressions
//            return node;
//        } else {
//            JexlNode newNode = (JexlNode) node;
//            JexlNode childNode = null;
//            /**
//             * Explore the possibility that we have an unnecessary top level ASTReference expression. Could walk up the tree, but this will be less work as
//             * we're checking if we're the root, then advance to see if we've within a Reference expression.
//             */
//            if (null == newNode.jjtGetParent() && (childNode = advanceReferenceExpression(newNode)) != null) {
//                if (childNode.jjtGetNumChildren() == 1)
//                    return childNode.jjtGetChild(0);
//
//            }
//            return newNode;
//        }
//    }
//
//    @Override
//    public Object visit(ASTReferenceExpression node, Object data) {
//        ASTReferenceExpression newExpressive = null;
//        if (!removeReferences) {
//            return node;
//        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(node) || ExceededTermThresholdMarkerJexlNode.instanceOf(node)
//                        || ExceededOrThresholdMarkerJexlNode.instanceOf(node)) {
//            return node;
//        } else if (JexlASTHelper.dereference(node) instanceof ASTAssignment) {
//            return node;
//        } else if (node.jjtGetParent() instanceof ASTAndNode) {
//            ASTAndNode andNode = JexlNodes.newInstanceOfType(((ASTAndNode) (node.jjtGetParent())));
//            andNode.image = node.jjtGetParent().image;
//
//            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//                JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
//                newNode.jjtSetParent(andNode);
//                andNode.jjtAddChild(newNode, andNode.jjtGetNumChildren());
//            }
//
//            return andNode;
//        } else if (node.jjtGetParent() instanceof ASTOrNode) {
//            ASTOrNode orNode = JexlNodes.newInstanceOfType(((ASTOrNode) (node.jjtGetParent())));
//            orNode.image = node.jjtGetParent().image;
//
//            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//                JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
//                newNode.jjtSetParent(orNode);
//                orNode.jjtAddChild(newNode, orNode.jjtGetNumChildren());
//            }
//
//            return orNode;
//        } else if (node.jjtGetParent() instanceof ASTNotNode || node.jjtGetParent() instanceof ASTReference) {
//            // ensure we keep negated expressions
//            return node;
//        } else if (node.jjtGetNumChildren() == 1 && (newExpressive = advanceReferenceExpression(node.jjtGetChild(0))) != null) {
//            return visit(newExpressive, data);
//        }
//
//        return node;
//
//    }
    
    private Object visitRef(JexlNode node, Object data) {
        if (!removeReferences) {
            return node;
        } else if (JexlASTHelper.dereference(node) instanceof ASTAssignment) {
            return node;
        } else if (node.jjtGetParent() instanceof ASTAndNode || node.jjtGetParent() instanceof ASTOrNode) {
            JexlNode parentNode = JexlNodes.newInstanceOfType(node.jjtGetParent());
            parentNode.image = node.jjtGetParent().image;

            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
                newNode.jjtSetParent(parentNode);
                parentNode.jjtAddChild(newNode, parentNode.jjtGetNumChildren());
            }

            return parentNode;
        } else if (node.jjtGetParent() instanceof ASTNotNode) {
            // ensure we keep negated expressions
            return node;
        } else {
            // let this node's child take it's place
            JexlNode parent = node.jjtGetParent();
            JexlNode newChild = node.jjtGetChild(0);

            newChild.jjtSetParent(parent);

            for (int nodeIdx = 0; nodeIdx < parent.jjtGetNumChildren(); nodeIdx++) {
                if (parent.jjtGetChild(nodeIdx).equals(node)) {
                    parent.jjtAddChild(newChild, nodeIdx);
                    break;
                }
            }

            return newChild;
        }
    }
    
    /**
     * Advances a child reference expression, if one is embedded Ex. {@code Ref RefExpr <-- this way we at Ref RefExpr}
     * <p>
     * will become
     * <p>
     * {@code Ref RefExpr <-- this still way we at}
     *
     * @param jexlNode
     *            Incoming JexlNode
     * @return
     */
    protected ASTReferenceExpression advanceReferenceExpression(JexlNode jexlNode) {
        
        if (jexlNode instanceof ASTReference) {
            
            if (jexlNode.jjtGetNumChildren() == 1 && jexlNode.jjtGetChild(0) instanceof ASTReferenceExpression) {
                ASTReferenceExpression expression = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
                if (null != jexlNode.jjtGetParent())
                    expression.image = jexlNode.jjtGetParent().image;
                else
                    expression.image = null;
                
                JexlNode node = jexlNode.jjtGetChild(0);
                
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode newNode = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
                    newNode.jjtSetParent(expression);
                    expression.jjtAddChild(newNode, expression.jjtGetNumChildren());
                }
                
                return expression;
            }
        }
        return null;
    }

    protected JexlNode flattenTree(JexlNode currentNode, Object data) {
        return flattenTreeIterative(currentNode, data);
    }

    protected JexlNode flattenTreeIterative(JexlNode currentNode, Object data) {

        Deque<JexlNode> children = new LinkedList<>();
        Deque<JexlNode> stack = new LinkedList<>();
        
        for (JexlNode child : children(currentNode))
            stack.push(child);

        boolean isFlatter = false;

        while (!stack.isEmpty()) {
            JexlNode node = stack.pop();
            JexlNode dereferenced = JexlASTHelper.dereference(node);
            
            if (acceptableNodesToCombine(currentNode, dereferenced, !node.equals(dereferenced))) {
                isFlatter = true;
                if (dereferenced.getClass().equals(currentNode.getClass())) {
                    for (int i = 0; i < dereferenced.jjtGetNumChildren(); i++) {
                        stack.push(dereferenced.jjtGetChild(i));
                    }
                } else {
                    children.push(dereferenced);
                }
            } else {
                children.push(node);
            }
        }

        if (isFlatter) {
            System.out.println("Creating a new node in flattenTree.  children: " + children.size());
            newNodesCreated++;

            JexlNode newNode = newInstanceOfType(currentNode);
            newNode.image = currentNode.image;
            newNode.jjtSetParent(currentNode.jjtGetParent());

            while (!children.isEmpty()) {
                JexlNode child = children.pop();
                newNode.jjtAddChild(child, newNode.jjtGetNumChildren());
                child.jjtSetParent(newNode);
            }

            return newNode;
        } else {
            return currentNode;
        }
    }

    protected boolean acceptableNodesToCombine(JexlNode currentNode, JexlNode newNode, boolean isWrapped) {
        return acceptableNodesToCombineNew(currentNode, newNode, isWrapped);
    }

    protected boolean acceptableNodesToCombineNew(JexlNode currentNode, JexlNode newNode, boolean isWrapped) {
        if ((currentNode instanceof ASTAndNode && !(newNode instanceof ASTOrNode)) || (currentNode instanceof ASTOrNode && !(newNode instanceof ASTAndNode))) {
            // if this is a bounded range or marker node, then do not combine
            if (newNode instanceof ASTAndNode && isBoundedRange((ASTAndNode) newNode)) {
                return false;
            }
            // don't allow combination of a marker node UNLESS it's already unwrapped
            else if (newNode instanceof ASTAndNode && QueryPropertyMarker.instanceOf(newNode, null) && isWrapped) {
                return false;
            }
            
            return true;
        }
        
        return false;
    }


    protected boolean acceptableNodesToCombineOrig(JexlNode currentNode, JexlNode newNode, boolean isWrapped) {
        if (currentNode.getClass().equals(newNode.getClass())) {
            // if this is a bounded range or marker node, then to not combine
            if (newNode instanceof ASTAndNode && isBoundedRange((ASTAndNode) newNode)) {
                return false;
            }
            // don't allow combination of a marker node UNLESS it's already unwrapped
            else if (newNode instanceof ASTAndNode && QueryPropertyMarker.instanceOf(newNode, null) && isWrapped) {
                return false;
            }

            return true;
        }

        return false;
    }
}
