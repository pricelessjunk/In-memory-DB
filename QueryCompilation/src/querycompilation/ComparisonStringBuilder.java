/*
 * Copyright(c) 2012 Saarland University - Information Systems Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package querycompilation;

import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Operator;
import dbs_project.storage.Type;
import java.util.ArrayList;
import java.util.List;
import static querycompilation.CodeCompileVisitorExample.*;

/**
 * This is a little helper class to keep track of the state of a comparison expression. This makes our life easier when 
 * we need when visiting the corresponding literals.
 *
 * @author Stefan Richter
 */
final class ComparisonStringBuilder {
    
    // Constants for the generated code
    static final String M_NAME_EQUALS = "equals";
    static final String M_NAME_COMPARE = "compareTo";
    
    //list of operands
    private final List<Constant> operands = new ArrayList<>();
    //the comparison operator
    private Operator operator;
    //the type of the current comparison
    private Type type;

    /**
     * Generate a String that contains a Java code for a comparison op from the
     * current state.
     *
     * @return Java code for a comparison expression, generated from current
     * state
     */
    public String build() {
        String result = "";
        if (operator != null) {
            if (operands.size() > 1) {//operations are at least binary
                for (int i = 1; i < operands.size(); ++i) {
                    if (result.length() > 0) {
                        result += " " + Operator.AND + " ";
                    }
                    String lhs = constantToString(operands.get(i - 1));
                    String rhs = constantToString(operands.get(i));
                    if (getType() == Type.STRING) {
                        result += lhs + ".";
                        if (getOperator() == Operator.EQ) {
                            result += M_NAME_EQUALS + "(" + rhs + ")";
                        } else {
                            result += M_NAME_COMPARE + "(" + rhs + ") " + getOperator() + " 0";
                        }
                    } else {
                        result += lhs + " " + getOperator() + " " + rhs;
                    }
                }
            } else {
                throw new IllegalStateException("Operation " + operator + " needs two or more operands. Found: " + operands);
            }
        }
        return result;
    }

    /**
     * Helper method. Translates a constant into a valid Java code String 
     * representation.
     *
     * @param c
     * @return Java code representation according to the constant's type.
     */
    private String constantToString(Constant c) {
        switch (c.getType()) {
            case COLUMN_NAME:
                return c.getValue() + "[" + V_NAME_ROWID + "]";//array access
            case VALUE_LITERAL:
                return getType() == Type.STRING ? "\"" + c.getValue() + "\"" : c.getValue();//quote strings
            default:
                throw new IllegalStateException("Unsupported type: " + c.getType());
        }
    }

    /**
     * Clear the current state, so that instances can be reused.
     */
    public void clear() {
        operator = null;
        type = null;
        operands.clear();
    }

    /**
     * Add Constant c to the current state.
     *
     * @param c
     */
    public void addConstant(Constant c) {
        operands.add(c);
    }

    /**
     *
     * @return the operator
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * Set the operator for the current state. Different operators within on
     * comparison expression indicate an error.
     *
     * @param operator the operator to set
     */
    public void setOperator(Operator operator) {
        if (this.operator != null && this.operator != operator) {
            throw new IllegalStateException("Different operators from the same comparison expression: " + this.operator + " and " + operator);
        } else {
            this.operator = operator;
        }
    }

    /**
     * @return the type
     */
    public Type getType() {
        return type;
    }

    /**
     * Set the value type used in this comparison expression. Different types
     * within on comparison expression indicate an error.
     *
     * @param type the type to set
     */
    public void setType(Type type) {
        if (this.type != null && this.type != type) {
            throw new IllegalStateException("Different types on same expression: " + this.type + " and " + type);
        } else {
            this.type = type;
        }
    }
}
