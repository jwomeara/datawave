package datawave.query.jexl;

import org.apache.commons.jexl2.JexlArithmetic;

public class NormalizedValueArithmetic extends JexlArithmetic {
    public NormalizedValueArithmetic() {
        this(false);
    }
    
    public NormalizedValueArithmetic(boolean lenient) {
        super(lenient);
    }
    
    @Override
    public boolean matches(Object left, Object right) {
        return true;
    }
    
    @Override
    public boolean equals(Object left, Object right) {
        return left.toString().compareTo(right.toString()) == 0;
    }
    
    @Override
    public boolean lessThan(Object left, Object right) {
        return left.toString().compareTo(right.toString()) < 0;
    }
    
    @Override
    public boolean greaterThan(Object left, Object right) {
        return left.toString().compareTo(right.toString()) > 0;
    }
    
    @Override
    public boolean lessThanOrEqual(Object left, Object right) {
        return left.toString().compareTo(right.toString()) <= 0;
    }
    
    @Override
    public boolean greaterThanOrEqual(Object left, Object right) {
        return left.toString().compareTo(right.toString()) >= 0;
    }
}
