package datawave.query.util;

import datawave.query.jexl.JexlNodeFactory;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    
    public CompositeRange(String compositeName) {
        super(compositeName);
    }
    
    public static CompositeRange clone(Composite other) {
        final CompositeRange clone = new CompositeRange(other.compositeName);
        for (String fieldName : other.fieldNameList) {
            clone.fieldNameList.add(new String(fieldName));
        }
        
        for (JexlNode jexlNode : other.jexlNodeList) {
            clone.jexlNodeList.add(jexlNode);
            if (!(other instanceof CompositeRange)) {
                clone.jexlNodeListLowerBound.add(jexlNode);
                clone.jexlNodeListUpperBound.add(jexlNode);
            }
        }
        
        for (String expression : other.expressionList) {
            clone.expressionList.add(new String(expression));
            if (!(other instanceof CompositeRange)) {
                clone.expressionListLowerBound.add(new String(expression));
                clone.expressionListUpperBound.add(new String(expression));
            }
        }
        
        if (other instanceof CompositeRange) {
            CompositeRange otherRange = (CompositeRange) other;
            for (JexlNode jexlNode : otherRange.jexlNodeListLowerBound) {
                clone.jexlNodeListLowerBound.add(jexlNode);
            }
            
            for (String expression : otherRange.expressionListLowerBound) {
                clone.expressionListLowerBound.add(new String(expression));
            }
            
            for (JexlNode jexlNode : otherRange.jexlNodeListUpperBound) {
                clone.jexlNodeListUpperBound.add(jexlNode);
            }
            
            for (String expression : otherRange.expressionListUpperBound) {
                clone.expressionListUpperBound.add(new String(expression));
            }
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
    
    public JexlNode getLowerBoundNode() {
        boolean isUnbounded = isLowerUnbounded();
        JexlNode node = getNodeClass(jexlNodeListLowerBound);
        // TODO: Revisit this
        if (isUnbounded && node instanceof ASTGTNode)
            return JexlNodeFactory.buildNode((ASTGENode) null, compositeName, "");
        return node;
    }
    
    public JexlNode getUpperBoundNode() {
        boolean isUnbounded = isUpperUnbounded();
        JexlNode node = getNodeClass(jexlNodeListUpperBound);
        // TODO: Revisit this
        if (isUnbounded && node instanceof ASTLENode)
            return JexlNodeFactory.buildNode((ASTLTNode) null, compositeName, "");
        return node;
    }
    
    private JexlNode getNodeClass(List<JexlNode> jexlNodeList) {
        JexlNode lastNode = null;
        for (JexlNode node : jexlNodeList) {
            if (node != null)
                lastNode = node;
        }
        return lastNode;
    }
    
    public String getLowerBoundExpression() {
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
                String inclusiveLowerBound = CompositeRange.getInclusiveLowerBound(expression);
                
                // if the length of the term changed, use the original exclusive
                // bound, and signal that this is the last expression
                if (inclusiveLowerBound.length() != expression.length())
                    lastNode = true;
                else
                    expression = inclusiveLowerBound;
            } else if (isUnbounded && node instanceof ASTGTNode && nextExpression == null) {
                expression = CompositeRange.getInclusiveLowerBound(expression);
                lastNode = true;
            }
            
            if (expression != null) {
                if (i > 0)
                    buf.append(START_SEPARATOR);
                
                buf.append(expression);
            } else {
                break;
            }
            
            if (lastNode)
                break;
        }
        return buf.toString();
    }
    
    public String getUpperBoundExpression() {
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
                String inclusiveUpperBound = CompositeRange.getInclusiveUpperBound(expression);
                
                // if the length of the term changed, use the original exclusive
                // bound, and signal that this is the last expression
                if (inclusiveUpperBound.length() != expression.length())
                    lastNode = true;
                else
                    expression = inclusiveUpperBound;
            } else if (isUnbounded && node instanceof ASTLENode && nextExpression == null) {
                expression = CompositeRange.getExclusiveUpperBound(expression);
                lastNode = true;
            }
            
            if (expression != null) {
                if (i > 0)
                    buf.append(START_SEPARATOR);
                
                buf.append(expression);
            } else {
                break;
            }
            
            if (lastNode)
                break;
        }
        return buf.toString();
    }
    
    public static String getInclusiveLowerBound(String lowerBound) {
        return incrementBound(lowerBound);
    }
    
    public static String getExclusiveLowerBound(String lowerBound) {
        return decrementBound(lowerBound);
    }
    
    public static String getInclusiveUpperBound(String upperBound) {
        return decrementBound(upperBound);
    }
    
    public static String getExclusiveUpperBound(String upperBound) {
        return incrementBound(upperBound);
    }
    
    public static String decrementBound(String orig) {
        // decrement string
        int[] codePoints = orig.codePoints().toArray();
        int length = codePoints.length;
        int lastCodePoint = codePoints[length - 1];
        if (lastCodePoint == Character.MIN_CODE_POINT) {
            length = Math.max(1, length - 1);
        } else {
            // keep decrementing until we reach a calid code point
            while (!Character.isValidCodePoint(--lastCodePoint))
                ;
            codePoints[length - 1] = lastCodePoint;
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.appendCodePoint(codePoints[i]);
        
        return sb.toString();
    }
    
    public static String incrementBound(String orig) {
        // increment string
        int[] codePoints = orig.codePoints().toArray();
        int length = codePoints.length;
        int lastCodePoint = codePoints[length - 1];
        boolean isMaxedOut = false;
        while (lastCodePoint == Character.MAX_CODE_POINT) {
            if (length == 1) {
                isMaxedOut = true;
                break;
            }
            lastCodePoint = codePoints[--length - 1];
        }
        
        // this means that the entire string consisted of MAX_CODE_POINT characters
        if (isMaxedOut) {
            codePoints = Arrays.copyOf(codePoints, codePoints.length + 1);
            codePoints[codePoints.length - 1] = Character.MIN_CODE_POINT;
            length = codePoints.length;
        } else {
            // keep incrementing until we reach a valid code point
            while (!Character.isValidCodePoint(++lastCodePoint))
                ;
            codePoints[length - 1] = lastCodePoint;
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.appendCodePoint(codePoints[i]);
        
        return sb.toString();
    }
    
    // this composite is invalid if:
    // - it contains a 'regex' node in any position
    // - it contains a 'not equals' node in any position
    @Override
    public boolean isValid() {
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
    
    // Note: For composite ranges, the anded node is NOT a goner IF either:
    // 1) the node is not present in the composite
    // 2) the node is present in the composite, and is NOT the first non-ASTEQNode in the composite
    // if this node is preceeded by all ASTEQNodes, then we can guarantee that all results returned by the
    // scan will be within range for this term. Otherwise, we do not have that guarantee.
    // why is this the case? because the scan range is based off of the first term, which ensures
    // that all of the terms we evaluate will be within range. However, for subsequent terms, there is no
    // guarantee that they will all be within that term's range. Because of that, we perform field index
    // filtering against those subsequent terms within the query iterator. ASTEQNodes work out as preceeding
    // nodes because they are operating against a fixed value, not a range.
    public boolean isGoner(JexlNode node) {
        boolean isGoner = true;
        int nodeIdx = jexlNodeList.indexOf(node);
        if (nodeIdx >= 0) {
            // If this node is preceeded by all ASTEQNodes, or it is the first
            // term in the composite, then we can throw it out because then all
            // of the values returned by our scan will be within range for this term
            for (int i = 0; i < nodeIdx; i++) {
                if (!(jexlNodeList.get(i) instanceof ASTEQNode)) {
                    // If any of the preceeding nodes is NOT an ASTEQNode, then our scan is not guaranteed
                    // to strictly return results that fall within range for this term
                    isGoner = false;
                    break;
                }
            }
        } else {
            // If the node is not present, it can't be a goner
            isGoner = false;
        }
        return isGoner;
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
