package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTUnknownFieldERNode;
import org.apache.commons.jexl2.parser.ASTUnsatisfiableERNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.SimpleNode;

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
        
        return (T) visitor.flattenInternal(node);
    }
    
    @Override
    public ASTJexlScript apply(ASTJexlScript input) {
        return flattenInternal(input);
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTIntegerLiteral node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTUnknownFieldERNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(ASTUnsatisfiableERNode node, Object data) {
        return flattenInternal(node);
    }
    
    @Override
    public Object visit(SimpleNode node, Object data) {
        throw new RuntimeException("NOT SURE WHAT TO DO ABOUT THIS!?!?!?!?!");
    }
}
