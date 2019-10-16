package datawave.query.jexl.visitors;

import com.google.common.collect.Lists;
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
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.commons.jexl2.parser.JexlNodes.children;

/**
 * This will flatten ands and ors. If requested this will also remove reference expressions and references where possible. NOTE: If you remove reference
 * expressions and references, this will adversely affect the jexl evaluation of the query.
 */
public class TreeFlatteningRebuildingVisitorNew extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(TreeFlatteningRebuildingVisitorNew.class);
    private boolean removeReferences = false;
    
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
        return visitor.flattenInternal(rootNode);
    }
    
    protected <T extends JexlNode> T flattenInternal(T rootNode) {
        // add all the nodes to the stack and iterate...
        Deque<JexlNode> postOrderStack = new LinkedList<>();
        Deque<JexlNode> workingStack = new LinkedList<>();
        workingStack.push(rootNode);
        
        // first, compute the post order traversal of all of the nodes.
        while (!workingStack.isEmpty()) {
            JexlNode node = workingStack.pop();
            
            postOrderStack.push(node);
            
            if (node.jjtGetNumChildren() > 0) {
                for (JexlNode child : children(node)) {
                    workingStack.push(child);
                }
            }
        }
        
        T newRoot = null;
        
        Deque<JexlNode> parentStack = new LinkedList<>();
        Deque<List<JexlNode>> childrenStack = new LinkedList<>();
        
        // now that we have the post order traversal, we can operate on the nodes...
        while (!postOrderStack.isEmpty()) {
            JexlNode node = postOrderStack.pop();
            
            boolean lastNode = node.equals(rootNode);
            
            JexlNode newNode = null;
            if (node instanceof ASTReference) {
                newNode = (JexlNode) visitRef(createNodeWithChildren(parentStack.pop(), childrenStack.pop()), null);
            } else if (node instanceof ASTReferenceExpression) {
                newNode = (JexlNode) visitRef(createNodeWithChildren(parentStack.pop(), childrenStack.pop()), null);
            } else if (node instanceof ASTOrNode) {
                newNode = flattenTreeIterative(parentStack.pop(), childrenStack.pop(), null);
            } else if (node instanceof ASTAndNode) {
                newNode = flattenTreeIterative(parentStack.pop(), childrenStack.pop(), null);
            } else {
                if (node.equals(parentStack.peek())) {
                    newNode = createNodeWithChildren(parentStack.pop(), childrenStack.pop());
                } else {
                    newNode = copy(node);
                }
            }
            
            if (!lastNode) {
                if (!node.jjtGetParent().equals(parentStack.peek())) {
                    parentStack.push(node.jjtGetParent());
                    childrenStack.push(Lists.newArrayList(newNode));
                } else {
                    childrenStack.peek().add(newNode);
                }
            } else {
                newRoot = (T) newNode;
            }
        }
        
        return (T) newRoot;
    }
    
    private JexlNode createNodeWithChildren(JexlNode prevNode, Collection<JexlNode> children) {
        JexlNode newNode = JexlNodes.newInstanceOfType(prevNode);
        newNode.image = prevNode.image;
        
        JexlNodes.ensureCapacity(newNode, children.size());
        
        int nodeIdx = 0;
        for (JexlNode child : children) {
            child.jjtSetParent(newNode);
            newNode.jjtAddChild(child, nodeIdx++);
        }
        
        newNode.jjtSetParent(prevNode.jjtGetParent());
        
        return newNode;
    }
    
    private boolean isBoundedRange(ASTAndNode node) {
        List<JexlNode> otherNodes = new ArrayList<>();
        Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic(node, otherNodes, false);
        if (ranges.size() == 1 && otherNodes.isEmpty()) {
            return true;
        }
        return false;
    }
    
    private Object visitRef(JexlNode node, Object data) {
        if (!removeReferences) {
            return node;
        } else if (JexlASTHelper.dereference(node) instanceof ASTAssignment) {
            return node;
        } else if (node.jjtGetParent() instanceof ASTAndNode || node.jjtGetParent() instanceof ASTOrNode) {
            return createNodeWithChildren(node.jjtGetParent(), Arrays.asList(children(node)));
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
    
    protected JexlNode flattenTreeIterative(JexlNode currentNode, List<JexlNode> childNodes, Object data) {
        
        if (!(currentNode instanceof ASTAndNode || currentNode instanceof ASTOrNode)) {
            log.error("Only ASTAndNodes and ASTOrNodes can be flattened!");
            throw new RuntimeException("Only ASTAndNodes and ASTOrNodes can be flattened!");
        }
        
        Deque<JexlNode> children = new LinkedList<>();
        Deque<JexlNode> stack = new LinkedList<>();
        
        for (JexlNode child : childNodes)
            stack.push(child);
        
        while (!stack.isEmpty()) {
            JexlNode node = stack.pop();
            JexlNode dereferenced = JexlASTHelper.dereference(node);
            
            if (acceptableNodesToCombine(currentNode, dereferenced, !node.equals(dereferenced))) {
                for (int i = 0; i < dereferenced.jjtGetNumChildren(); i++) {
                    stack.push(dereferenced.jjtGetChild(i));
                }
            } else {
                children.push(node);
            }
        }
        
        return createNodeWithChildren(currentNode, children);
    }
    
    protected boolean acceptableNodesToCombine(JexlNode currentNode, JexlNode newNode, boolean isWrapped) {
        return acceptableNodesToCombineOrig(currentNode, newNode, isWrapped);
    }
    
    protected boolean acceptableNodesToCombineNew(JexlNode currentNode, JexlNode newNode, boolean isWrapped) {
        if ((currentNode instanceof ASTAndNode && !(newNode instanceof ASTOrNode)) || (currentNode instanceof ASTOrNode && !(newNode instanceof ASTAndNode))) {
            // if this is a bounded range or a wrapped marker node, then do not combine
            if (newNode instanceof ASTAndNode && (isBoundedRange((ASTAndNode) newNode) || (QueryPropertyMarker.instanceOf(newNode, null) && isWrapped))) {
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
            // don't allow marked new nodes to be flattened into a parent node UNLESS the new node is already unwrapped
            else if (newNode instanceof ASTAndNode && QueryPropertyMarker.instanceOf(newNode, null) && isWrapped) {
                return false;
            }
            // don't allow new nodes to be flattened into a marked parent node UNLESS the new node is already unwrapped
            else if (newNode instanceof ASTAndNode && QueryPropertyMarker.instanceOf(currentNode, null) && isWrapped) {
                return false;
            }
            
            return true;
        }
        
        return false;
    }
}
