package datawave.query.composite;

import com.google.common.collect.Sets;
import datawave.query.Constants;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;

import java.util.Arrays;
import java.util.Set;

/**
 * This class contains a collection of methods and constants which are used when dealing with composite terms and ranges.
 *
 */
public class CompositeUtils {
    
    public static final String SEPARATOR = Constants.MAX_UNICODE_STRING;
    public static final Set<Class<?>> INVALID_LEAF_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTNENode.class);
    public static final Set<Class<?>> VALID_LEAF_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTEQNode.class, ASTERNode.class, ASTGTNode.class, ASTGENode.class,
                    ASTLTNode.class, ASTLENode.class, ASTAndNode.class);
    
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
    
    // NOTE: The output string will have the same number of characters as the input string.
    // An exception will be thrown if we can't decrement without maintaining the number of characters
    public static String decrementBound(String orig) {
        // decrement string
        int[] codePoints = orig.codePoints().toArray();
        int length = codePoints.length;
        int lastCodePoint = codePoints[length - 1];
        while (lastCodePoint == Character.MIN_CODE_POINT) {
            if (length == 1)
                throw new RuntimeException("Cannot decrement bound without decreasing the number of characters.");
            codePoints[length - 1] = Character.MAX_CODE_POINT;
            lastCodePoint = codePoints[--length - 1];
        }
        
        // keep decrementing until we reach a valid code point
        while (!Character.isValidCodePoint(--lastCodePoint))
            ;
        codePoints[length - 1] = lastCodePoint;
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < codePoints.length; i++)
            sb.appendCodePoint(codePoints[i]);
        
        return sb.toString();
    }
    
    // NOTE: The output string will have the same number of characters as the input string.
    // An exception will be thrown if we can't increment without maintaining the number of characters
    public static String incrementBound(String orig) {
        // increment string
        int[] codePoints = orig.codePoints().toArray();
        int length = codePoints.length;
        int lastCodePoint = codePoints[length - 1];
        while (lastCodePoint == Character.MAX_CODE_POINT) {
            if (length == 1)
                throw new RuntimeException("Cannot increment bound without increasing the number of characters.");
            codePoints[length - 1] = Character.MIN_CODE_POINT;
            lastCodePoint = codePoints[--length - 1];
        }
        
        // keep incrementing until we reach a valid code point
        while (!Character.isValidCodePoint(++lastCodePoint))
            ;
        codePoints[length - 1] = lastCodePoint;
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < codePoints.length; i++)
            sb.appendCodePoint(codePoints[i]);
        
        return sb.toString();
    }
}
