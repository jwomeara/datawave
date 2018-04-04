package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTCompositePredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CompositePredicateVisitor extends BaseVisitor {
    
    private List<String> compFields = new ArrayList<>();
    private Set<JexlNode> compositePredicates = new HashSet<>();
    
    private CompositePredicateVisitor() {}
    
    /**
     * Finds the ASTCompositePredicate associated with the given composite field
     *
     * @param script
     *            An ASTJexlScript
     * @return
     */
    public static Set<JexlNode> findCompositePredicates(JexlNode script, Collection<String> compFields) {
        CompositePredicateVisitor visitor = new CompositePredicateVisitor();
        visitor.compFields.addAll(compFields);
        
        script.jjtAccept(visitor, null);
        return visitor.compositePredicates;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (data == null && ASTCompositePredicate.instanceOf(node)) {
            Set<String> foundIdentifiers = new HashSet<>();
            node.childrenAccept(this, foundIdentifiers);
            
            if (foundIdentifiers.size() == compFields.size() && foundIdentifiers.containsAll(compFields))
                compositePredicates.add(node);
            return null;
        }
        return node.childrenAccept(this, data);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (data == null && ASTCompositePredicate.instanceOf(node)) {
            Set<String> foundIdentifiers = new HashSet<>();
            node.childrenAccept(this, foundIdentifiers);
            
            if (foundIdentifiers.size() == compFields.size() && foundIdentifiers.containsAll(compFields))
                compositePredicates.add(node);
            return null;
        }
        return node.childrenAccept(this, data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        if ((data != null) && (data instanceof Set)) {
            Set<String> foundIdentifiers = (Set<String>) data;
            foundIdentifiers.add(JexlASTHelper.getIdentifier(node));
        }
        return null;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        if ((data != null) && (data instanceof Set)) {
            Set<String> foundIdentifiers = (Set<String>) data;
            foundIdentifiers.add(JexlASTHelper.getIdentifier(node));
        }
        return null;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        if ((data != null) && (data instanceof Set)) {
            Set<String> foundIdentifiers = (Set<String>) data;
            foundIdentifiers.add(JexlASTHelper.getIdentifier(node));
        }
        return null;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        if ((data != null) && (data instanceof Set)) {
            Set<String> foundIdentifiers = (Set<String>) data;
            foundIdentifiers.add(JexlASTHelper.getIdentifier(node));
        }
        return null;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        if ((data != null) && (data instanceof Set)) {
            Set<String> foundIdentifiers = (Set<String>) data;
            foundIdentifiers.add(JexlASTHelper.getIdentifier(node));
        }
        return null;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        if ((data != null) && (data instanceof Set)) {
            Set<String> foundIdentifiers = (Set<String>) data;
            foundIdentifiers.add(JexlASTHelper.getIdentifier(node));
        }
        return null;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        if ((data != null) && (data instanceof Set)) {
            Set<String> foundIdentifiers = (Set<String>) data;
            foundIdentifiers.add(JexlASTHelper.getIdentifier(node));
        }
        return null;
    }
}
