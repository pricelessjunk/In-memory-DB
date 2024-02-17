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

import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.ExpressionElement;
import dbs_project.query.predicate.Operator;
import dbs_project.query.predicate.example.ExpressionVisitorExampleImpl;
import dbs_project.query.predicate.impl.Constants;
import dbs_project.query.predicate.impl.Expressions;
import dbs_project.storage.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main class of the small educational example on i) code generation/compilation and ii) the visitor pattern.
 *
 * (Must have javassist-[version].jar and DBMS Project classes in classpath! One approach is to just copy this into your
 * project.)
 *
 * @author Stefan Richter
 */
public class QueryCompilationExample {

    private static final int ROW_COUNT = 1024 * 1024 * 10;

    public static void main(String[] args) throws Exception {
        //generate some data and schema
        final Map<String, ColumnDescriptor> testData = generateTestSchemaAndData();
        //generate a test expression tree
        final Expression testExpression = generateTestExpressionTree();
        //EXAMPLE 1: print visitor
        System.out.println("EXAMPLE 1: print visitor");
        final ExpressionVisitorExampleImpl printVisitorFromProjectExample = new ExpressionVisitorExampleImpl();
        testExpression.accept(printVisitorFromProjectExample);
        System.out.println("Printing expression:");
        System.out.println(printVisitorFromProjectExample);
        System.out.println();
        //EXAMPLE 2: compiler visitor
        System.out.println("EXAMPLE 2: compiler visitor");
        final CodeCompileVisitorExample compileVisitor = new CodeCompileVisitorExample(testData);
        testExpression.accept(compileVisitor);
        DynamicPredicate predicate = compileVisitor.createPredicate();
        System.out.println();
        //iterate the rows and check against the predicate
        for (int i = 0; i < ROW_COUNT; ++i) {
            if (predicate.eval(i)) {
                System.out.println("Match found at RID: " + i);
            }
        }
    }

    private static Map<String, ColumnDescriptor> generateTestSchemaAndData() {
        String[] name = new String[ROW_COUNT];
        int[] age = new int[ROW_COUNT];
        int[] test = new int[ROW_COUNT];
        double[] quota = new double[ROW_COUNT];
        //TODO: you could fill the arrays with generated random data, but for this example it is not relevant...
        //...for now, we just generate one matching row at one position
        int matchPosition = 42;
        age[matchPosition] = 21;
        test[matchPosition] = 19;
        name[matchPosition] = "Franz";
        quota[matchPosition] = 0.03d;
        //create schema
        final List<ColumnDescriptor> columnsDesc = new ArrayList<>();
        columnsDesc.add(new ColumnDescriptor("name", Type.STRING, name));
        columnsDesc.add(new ColumnDescriptor("age", Type.INTEGER, age));
        columnsDesc.add(new ColumnDescriptor("test", Type.INTEGER, test));
        columnsDesc.add(new ColumnDescriptor("quota", Type.DOUBLE, quota));
        final Map<String, ColumnDescriptor> schemaByName = new HashMap<>();
        for (ColumnDescriptor cd : columnsDesc) {
            schemaByName.put(cd.getColumnName(), cd);
        }
        return schemaByName;
    }

    private static Expression generateTestExpressionTree() {
        //a predicate expression example
        final Expression expCompareGreaterEquals = Expressions.createExpression(Operator.GEQ, new ArrayList<ExpressionElement>());
        final Expression expCompareEquals = Expressions.createExpression(Operator.EQ, new ArrayList<ExpressionElement>());
        final Expression expCompareGreater = Expressions.createExpression(Operator.GT, new ArrayList<ExpressionElement>());
        final Expression expOr = Expressions.createExpression(Operator.OR, new ArrayList<ExpressionElement>());
        final Expression expRootAnd = Expressions.createExpression(Operator.AND, new ArrayList<ExpressionElement>());
        expRootAnd.addOperand(expCompareGreaterEquals);
        expRootAnd.addOperand(expOr);
        expOr.addOperand(expCompareEquals);
        //
        expCompareGreaterEquals.addOperand(Constants.createColumnNameConstant("age"));
        expCompareGreaterEquals.addOperand(Constants.createColumnNameConstant("test"));
        expCompareGreaterEquals.addOperand(Constants.createLiteralConstant("18"));
        //
        expCompareEquals.addOperand(Constants.createColumnNameConstant("name"));
        expCompareEquals.addOperand(Constants.createLiteralConstant("Franz"));
        expCompareGreater.addOperand(Constants.createColumnNameConstant("quota"));
        expCompareGreater.addOperand(Constants.createLiteralConstant("0.05d"));
        expOr.addOperand(expCompareGreater);
        return expRootAnd;
    }
}
