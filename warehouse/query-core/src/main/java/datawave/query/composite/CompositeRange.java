package datawave.query.composite;

import datawave.data.type.DiscreteIndexType;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A composite range is a special type of composite which is used to create a single bounded or unbounded range from multiple terms. Composite ranges can only
 * be created when the base composite term produces ranges whose terms, and underlying data within that range are of fixed length.
 *
 */
public class CompositeRange extends Composite {
    
    public final List<JexlNode> jexlNodeListLowerBound = new ArrayList<>();
    public final List<JexlNode> jexlNodeListUpperBound = new ArrayList<>();
    public final List<String> expressionListLowerBound = new ArrayList<>();
    public final List<String> expressionListUpperBound = new ArrayList<>();
    
    public CompositeRange(String compositeName, String separator) {
        super(compositeName, separator);
    }
    
    public static CompositeRange clone(Composite other) {
        final CompositeRange clone = new CompositeRange(other.compositeName, other.separator);
        clone.fieldNameList.addAll(other.fieldNameList);
        
        for (JexlNode jexlNode : other.jexlNodeList) {
            clone.jexlNodeList.add(jexlNode);
            if (!(other instanceof CompositeRange)) {
                clone.jexlNodeListLowerBound.add(jexlNode);
                clone.jexlNodeListUpperBound.add(jexlNode);
            }
        }
        
        for (String expression : other.expressionList) {
            clone.expressionList.add(expression);
            if (!(other instanceof CompositeRange)) {
                clone.expressionListLowerBound.add(expression);
                clone.expressionListUpperBound.add(expression);
            }
        }
        
        if (other instanceof CompositeRange) {
            CompositeRange otherRange = (CompositeRange) other;
            clone.jexlNodeListLowerBound.addAll(otherRange.jexlNodeListLowerBound);
            clone.expressionListLowerBound.addAll(otherRange.expressionListLowerBound);
            clone.jexlNodeListUpperBound.addAll(otherRange.jexlNodeListUpperBound);
            clone.expressionListUpperBound.addAll(otherRange.expressionListUpperBound);
        }
        
        return clone;
    }
    
    @Override
    public CompositeRange clone() {
        return clone(this);
    }
    
    @Override
    public String toString() {
        return "CompositeRange [compositeName=" + compositeName + ", fieldNameList=" + fieldNameList + ", jexlNodeList=" + jexlNodeList + ", expressionList="
                        + expressionList + ", jexlNodeListLowerBound=" + jexlNodeListLowerBound + ", expressionListLowerBound=" + expressionListLowerBound
                        + ", jexlNodeListUpperBound=" + jexlNodeListUpperBound + ", expressionListUpperBound=" + expressionListUpperBound + "]";
    }
    
    private boolean isLowerUnbounded() {
        for (int i = 0; i < jexlNodeList.size(); i++)
            if (jexlNodeListLowerBound.get(i) == null && jexlNodeListUpperBound.get(i) != null)
                return true;
        return false;
    }
    
    private boolean isUpperUnbounded() {
        for (int i = 0; i < jexlNodeList.size(); i++)
            if (jexlNodeListUpperBound.get(i) == null && jexlNodeListLowerBound.get(i) != null)
                return true;
        return false;
    }
    
    @Override
    public void getNodesAndExpressions(List<Class<? extends JexlNode>> nodeClasses, List<String> expressions, Map<String,DiscreteIndexType<?>> discreteIndexTypeMap, boolean includeOldData) {
        if (includeOldData) {
            expressions.add(getFullyInclusiveLowerBoundExpression(discreteIndexTypeMap));
            nodeClasses.add(ASTGENode.class);
        } else {
            Class<? extends JexlNode> lowerBoundNodeClass = getLowerBoundNodeClass();
            String lowerBoundExpression = getLowerBoundExpression(discreteIndexTypeMap);
            if (lowerBoundNodeClass != null && !lowerBoundExpression.equals("")) {
                nodeClasses.add(lowerBoundNodeClass);
                expressions.add(lowerBoundExpression);
            }
        }
        Class<? extends JexlNode> upperBoundNodeClass = getUpperBoundNodeClass();
        String upperBoundExpression = getUpperBoundExpression(discreteIndexTypeMap);
        if (upperBoundNodeClass != null && !upperBoundExpression.equals("")) {
            nodeClasses.add(upperBoundNodeClass);
            expressions.add(upperBoundExpression);
        }
    }
    
    private Class<? extends JexlNode> getLowerBoundNodeClass() {
        boolean isUnbounded = isLowerUnbounded();
        Class<? extends JexlNode> nodeClass = getNodeClass(jexlNodeListLowerBound);
        if (isUnbounded && nodeClass.equals(ASTGTNode.class))
            return ASTGENode.class;
        return nodeClass;
    }
    
    private Class<? extends JexlNode> getUpperBoundNodeClass() {
        boolean isUnbounded = isUpperUnbounded();
        Class<? extends JexlNode> nodeClass = getNodeClass(jexlNodeListUpperBound);
        if (isUnbounded && nodeClass.equals(ASTLENode.class))
            return ASTLTNode.class;
        return nodeClass;
    }
    
    // used to handle special case where our index is overloaded and runs against legacy (i.e. non-composite) data
    private String getFullyInclusiveLowerBoundExpression(Map<String,DiscreteIndexType<?>> discreteIndexTypeMap) {
        String expression;
        if (jexlNodeListLowerBound.get(0) instanceof ASTGTNode)
            expression = CompositeUtils.getInclusiveLowerBound(expressionListLowerBound.get(0), discreteIndexTypeMap.get(fieldNameList.get(0)));
        else
            expression = expressionListLowerBound.get(0);
        return expression;
    }
    
    private String getLowerBoundExpression(Map<String,DiscreteIndexType<?>> discreteIndexTypeMap) {
        boolean isUnbounded = isLowerUnbounded();
        StringBuilder buf = new StringBuilder();
        boolean lastNode = false;
        for (int i = 0; i < expressionListLowerBound.size(); i++) {
            String expression = expressionListLowerBound.get(i);
            JexlNode node = jexlNodeListLowerBound.get(i);
            
            // we need to turn > into >=
            // if the next expression is null, then this is our last
            // node, so we don't need any special handling
            String nextExpression = ((i + 1) < expressionListLowerBound.size()) ? expressionListLowerBound.get(i + 1) : null;
            if (node instanceof ASTGTNode && i != (expressionListLowerBound.size() - 1) && nextExpression != null) {
                String inclusiveLowerBound = CompositeUtils.getInclusiveLowerBound(expression, discreteIndexTypeMap.get(fieldNameList.get(i)));
                
                // if the length of the term changed, use the original exclusive
                // bound, and signal that this is the last expression
                if (inclusiveLowerBound.length() != expression.length())
                    lastNode = true;
                else
                    expression = inclusiveLowerBound;
            } else if (isUnbounded && node instanceof ASTGTNode && nextExpression == null) {
                expression = CompositeUtils.getInclusiveLowerBound(expression, discreteIndexTypeMap.get(fieldNameList.get(i)));
                lastNode = true;
            }
            
            if (expression != null) {
                if (i > 0)
                    buf.append(separator);
                
                buf.append(expression);
            } else {
                break;
            }
            
            if (lastNode)
                break;
        }
        return buf.toString();
    }
    
    private String getUpperBoundExpression(Map<String,DiscreteIndexType<?>> discreteIndexTypeMap) {
        boolean isUnbounded = isUpperUnbounded();
        StringBuilder buf = new StringBuilder();
        boolean lastNode = false;
        for (int i = 0; i < expressionListUpperBound.size(); i++) {
            String expression = expressionListUpperBound.get(i);
            JexlNode node = jexlNodeListUpperBound.get(i);
            
            // we need to turn < into <=
            // if the next expression is null, then this is our last
            // node, so we don't need any special handling
            String nextExpression = ((i + 1) < expressionListUpperBound.size()) ? expressionListUpperBound.get(i + 1) : null;
            if (node instanceof ASTLTNode && i != (expressionListUpperBound.size() - 1) && nextExpression != null) {
                String inclusiveUpperBound = CompositeUtils.getInclusiveUpperBound(expression, discreteIndexTypeMap.get(fieldNameList.get(i)));
                
                // if the length of the term changed, use the original exclusive
                // bound, and signal that this is the last expression
                if (inclusiveUpperBound.length() != expression.length())
                    lastNode = true;
                else
                    expression = inclusiveUpperBound;
            } else if (isUnbounded && node instanceof ASTLENode && nextExpression == null) {
                expression = CompositeUtils.getExclusiveUpperBound(expression, discreteIndexTypeMap.get(fieldNameList.get(i)));
                lastNode = true;
            }
            
            if (expression != null) {
                if (i > 0)
                    buf.append(separator);
                
                buf.append(expression);
            } else {
                break;
            }
            
            if (lastNode)
                break;
        }
        return buf.toString();
    }
    
    // this composite is invalid if:
    // - it contains a 'regex' node in any position
    // - it contains a 'not equals' node in any position
    @Override
    public boolean isValid() {
        if (!super.isValid())
            return false;
        for (JexlNode node : jexlNodeList)
            if (node instanceof ASTERNode || node instanceof ASTNENode)
                return false;
        return true;
    }
    
    public boolean contains(JexlNode node) {
        boolean success = true;
        if (node instanceof ASTAndNode)
            for (int i = 0; i < node.jjtGetNumChildren(); i++)
                success &= this.jexlNodeListLowerBound.contains(node.jjtGetChild(i)) || this.jexlNodeListUpperBound.contains(node.jjtGetChild(i));
        else
            success = this.jexlNodeListLowerBound.contains(node) || this.jexlNodeListUpperBound.contains(node);
        return success;
    }
    
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((expressionListLowerBound == null) ? 0 : expressionListLowerBound.hashCode());
        result = prime * result + ((jexlNodeListLowerBound == null) ? 0 : jexlNodeListLowerBound.hashCode());
        result = prime * result + ((expressionListUpperBound == null) ? 0 : expressionListUpperBound.hashCode());
        result = prime * result + ((jexlNodeListUpperBound == null) ? 0 : jexlNodeListUpperBound.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (!super.equals(obj))
            return false;
        CompositeRange other = (CompositeRange) obj;
        if (expressionListLowerBound == null) {
            if (other.expressionListLowerBound != null)
                return false;
        } else if (!expressionListLowerBound.equals(other.expressionListLowerBound))
            return false;
        if (jexlNodeListLowerBound == null) {
            if (other.jexlNodeListLowerBound != null)
                return false;
        } else if (!jexlNodeListLowerBound.equals(other.jexlNodeListLowerBound))
            return false;
        if (expressionListUpperBound == null) {
            if (other.expressionListUpperBound != null)
                return false;
        } else if (!expressionListUpperBound.equals(other.expressionListUpperBound))
            return false;
        if (jexlNodeListUpperBound == null) {
            if (other.jexlNodeListUpperBound != null)
                return false;
        } else if (!jexlNodeListUpperBound.equals(other.jexlNodeListUpperBound))
            return false;
        return true;
    }
}
