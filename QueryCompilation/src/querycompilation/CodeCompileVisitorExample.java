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
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.ExpressionElement;
import dbs_project.query.predicate.ExpressionVisitor;
import dbs_project.query.predicate.Operator;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;

/**
 * Simple example on how to implement compilation for a predicate check in Java. The code uses the expression structure 
 * provided with the project.
 *
 * Notice that this implementation is just an educational example and does not cover the complete specification
 * for the project. Minor details like support for Date or Null values are not implemented because they add nothing to
 * illustrate the basic principle. 
 * 
 * Alternatively, you could also use the ASM library provided with the project for the same purpose. ASM may provide 
 * better performance/lower compilation time at the cost of more implementation effort.
 *
 * @author Stefan Richter
 */
public class CodeCompileVisitorExample implements ExpressionVisitor {

    // Output generated code?
    private static final boolean VERBOSE = true;
    // Generator for creating unique classnames
    private static final AtomicLong CLASS_SEQUENCE_GENERATOR;
    // Javassist classpool
    private static final ClassPool POOL;
    // Text constants for the generated code
    static final String V_NAME_ROWID = "rowId";
    static final String V_NAME_SCHEMA = "schema";
    static final String M_NAME_EVAL = "eval";
    static final String M_NAME_GET_ARR = "getColumnData";
    static final String INDENT = "    ";
    //Provided schema on which code generation is based
    private final Map<String, ColumnDescriptor> schema;
    //Collects the code for the eval function during visitation.
    private final StringBuilder evalCodeStringBuilder;
    //Keeps state when processing comparison expressions
    private final ComparisonStringBuilder comparisonStringBuilder;

    static {
        CLASS_SEQUENCE_GENERATOR = new AtomicLong(0);
        POOL = ClassPool.getDefault();
        POOL.importPackage(DynamicPredicate.class.getPackage().getName());
        POOL.importPackage(ColumnDescriptor.class.getPackage().getName());
        POOL.importPackage(Map.class.getPackage().getName());
    }

    /**
     * Initialize a new CodeCompileVisitor with a schema.
     *
     * @param schema
     */
    public CodeCompileVisitorExample(Map<String, ColumnDescriptor> schema) {
        this.evalCodeStringBuilder = new StringBuilder();
        this.comparisonStringBuilder = new ComparisonStringBuilder();
        this.schema = schema;
    }

    /**
     * Entry point: the visitor receives the expression to compile.
     * 
     * @param expression 
     */
    @Override
    public void visitExpression(Expression expression) {
        beginExpression();
        processSubExpressions(expression);
        endExpression();
    }

    @Override
    public void visitConstant(Constant constant) {
        switch (constant.getType()) {
            case COLUMN_NAME:
                ColumnDescriptor cd = schema.get(constant.getValue());
                comparisonStringBuilder.setType(cd.getType());
                break;
            case VALUE_LITERAL:
                break;
            case NULL_LITERAL:
            //TODO
            default:
                throw new IllegalArgumentException("Unsupported constant type: " + constant.getType());
        }
        comparisonStringBuilder.addConstant(constant);
    }

    /**
     * After visiting an expression, this returns the Java code fragment of a correponding eval-function.
     *
     * @return eval-function code
     */
    @Override
    public String toString() {
        return evalCodeStringBuilder.toString();
    }

    /**
     * This is where the magic happens, aka "we generate and load a new class with javassist + classloading".
     * 
     * @return @throws Exception
     */
    public DynamicPredicate createPredicate() throws Exception {
        String className = "DynamicPredicate_" + CLASS_SEQUENCE_GENERATOR.getAndIncrement();
        //make a new ct class
        CtClass ctDynClass = POOL.makeClass(className);
        //make it implement the DynamicPredicate interface
        ctDynClass.addInterface(POOL.get(DynamicPredicate.class.getCanonicalName()));
        //create all necessary fields from the schema
        generateFields(ctDynClass);
        //create a constructor that takes the schema
        generateContructor(ctDynClass);
        //create eval(...) method from the result of the visitor's AST traversal
        generateEvalMethod(ctDynClass);
        /**
         * Instantiate a new classloader for the dynamic class that has the context classloader as parent. Necessary to
         * get rid of the generated class later and to avoid running out of PermGen-memory. (-> OutOfMemoryError:
         * PermGen) However, you could imagine to cache classes or reuse a classloader to load serveral generated
         * classes.
         */
        ClassLoader cl = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
        };
        //transform the CtClass into a Class and load it
        Class<?> clazz = ctDynClass.toClass(cl, ColumnDescriptor.class.getProtectionDomain());
        //create instance by calling the created contructor
        Constructor<?> constructor = clazz.getConstructor(Map.class);
        return (DynamicPredicate) constructor.newInstance(schema);
    }

    private void beginExpression() {
        evalCodeStringBuilder.append('(');
    }

    private void processSubExpressions(Expression expression) {
        final Operator op = expression.getOperator();
        for (int i = 0; i < expression.getOperandCount(); ++i) {
            ExpressionElement node = expression.getOperand(i);
            if (i > 0) {//from the second node...
                if (isBooleanOperator(op)) {
                    //simply appent the to string representation
                    evalCodeStringBuilder.append(' ').append(op).append(' ');
                } else {//-> is comparison operator
                    //we must keep some state about the current operation
                    comparisonStringBuilder.setOperator(op);
                }
            }
            node.accept(this);
        }
    }

    private void endExpression() {
        evalCodeStringBuilder.append(comparisonStringBuilder.build());
        evalCodeStringBuilder.append(')');
        //clear state
        comparisonStringBuilder.clear();
    }

    private void generateFields(CtClass ctClass) throws CannotCompileException {
        if (VERBOSE) {
            System.out.println("// FIELDS");
        }
        for (ColumnDescriptor cd : schema.values()) {
            String fieldString = columnDescriptorToFieldString(cd);
            if (VERBOSE) {
                System.out.print(fieldString);
            }
            ctClass.addField(CtField.make(fieldString, ctClass));
        }
    }

    private void generateContructor(CtClass ctClass) throws CannotCompileException {
        //Note: javassist does not support generics!
        String constructorCode = "public " + ctClass.getSimpleName() + "(" + Map.class.getSimpleName() + " " + V_NAME_SCHEMA + ") {\n";
        for (ColumnDescriptor cd : schema.values()) {
            constructorCode += columnDescriptorToInitString(cd);
        }
        constructorCode += "}";
        if (VERBOSE) {
            System.out.println("// CONSTRUCTOR");
            System.out.println(constructorCode);
        }
        ctClass.addConstructor(CtNewConstructor.make(constructorCode, ctClass));
    }

    private void generateEvalMethod(CtClass ctClass) throws CannotCompileException {
        String evalMethod = "public boolean " + M_NAME_EVAL + "(int " + V_NAME_ROWID + ") {\n";
        evalMethod += INDENT + "return " + evalCodeStringBuilder.toString() + ";\n";
        evalMethod += "}";
        if (VERBOSE) {
            System.out.println("// EVAL METHOD");
            System.out.println(evalMethod);
        }
        ctClass.addMethod(CtNewMethod.make(evalMethod, ctClass));
    }

    private static boolean isBooleanOperator(Operator op) {
        return op == Operator.AND || op == Operator.OR;
    }

    /**
     * Translate a column descriptor into the code of a corresponding Java field declaration.
     *
     * @param cd
     * @return code of a corresponding Java field declaration
     */
    private static String columnDescriptorToFieldString(ColumnDescriptor cd) {
        return "private final " + cd.getType().getJavaClass().getSimpleName() + "[] " + cd.getColumnName() + ";\n";
    }

    /**
     * Translate a column descriptor into the code of a corresponding Java field initialization.
     *
     * @param cd
     * @return code of a corresponding Java field initialization
     */
    private static String columnDescriptorToInitString(ColumnDescriptor cd) {
        String initCode = INDENT + "this." + cd.getColumnName()
                + " = (" + cd.getType().getJavaClass().getSimpleName() + "[])(("
                + ColumnDescriptor.class.getSimpleName() + ")" + V_NAME_SCHEMA
                + ".get(\"" + cd.getColumnName() + "\"))." + M_NAME_GET_ARR
                + "();\n";
        return initCode;
    }
}
