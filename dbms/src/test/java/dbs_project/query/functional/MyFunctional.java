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
package dbs_project.query.functional;

import dbs_project.database.DatabaseFactory;
import dbs_project.exceptions.QueryExecutionException;
import dbs_project.index.IndexType;
import dbs_project.query.QueryLayer;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.Operator;
import dbs_project.query.predicate.impl.Constants;
import dbs_project.query.predicate.impl.Expressions;
import dbs_project.query.statement.*;
import dbs_project.storage.RowCursor;
import dbs_project.storage.Type;
import dbs_project.util.ResultComparator;
import dbs_project.util.SimpleColumn;
import dbs_project.util.TPCHData;
import dbs_project.util.TestTableBuilder;
import dbs_project.util.Utils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.junit.*;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class MyFunctional {

    private DerbyDB db;
    private QueryLayer qLayer;
    private static final String CUSTOMER = "CUSTOMER";
    private static final String CUSTOMER2 = "customer";
    private static final String NATION = "nation";
    private static final String REGION = "region";
    private static final String LINEITEM = "lineitem";
    private static final String ORDERS = "orders";
    private static final List<String> selectAll = Arrays.asList("*");
    private static final List<String> cust_columns = Arrays.asList(
            "c_custkey", "c_name", "c_address", "c_nationkey",
            "c_phone", "c_acctbal", "c_mktsegment", "c_comment");
    private static final List<Type> cust_types = Arrays.asList(
            Type.INTEGER, Type.STRING, Type.STRING, Type.INTEGER,
            Type.STRING, Type.DOUBLE, Type.STRING, Type.STRING);
    private static final List<String> nation_columns
            = Arrays.asList("n_nationkey", "n_name", "n_regionkey", "n_comment");
    private static final List<Type> nation_types
            = Arrays.asList(Type.INTEGER, Type.STRING, Type.INTEGER, Type.STRING);
    private static final List<String> region_columns
            = Arrays.asList("r_regionkey", "r_name", "r_comment");
    private static final List<Type> region_types
            = Arrays.asList(Type.INTEGER, Type.STRING, Type.STRING);
    private static final QueryStatement dummySQL = Statements.buildQueryStatement(
            Arrays.asList(CUSTOMER), Arrays.asList("*"), null);
    private static final List<String> simpleSelectPointQueryProjection = Arrays.asList("c_name", "c_address");
    private static final List<String> simpleSelectRangeQueryProjection = Arrays.asList("c_name");
    private static final List<String> simpleSelectJoinQueryProjection = Arrays.asList("c_name", "r_name");
    private static final List<String> cust_columns_update = Arrays.asList("c_comment");
    private static final List<String> cust_values_update
            = Arrays.asList("the next football worldcup is gonna be there");

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Utils.redirectStreams();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Utils.revertStreams();
    }

    @Before
    public void setUp() throws Exception {
        qLayer = DatabaseFactory.INSTANCE.createInstance().getQueryLayer();
        db = new DerbyDB();
    }

    @After
    public void tearDown() throws Exception {
        db.close();
    }

    /*
     * UNIT TESTS
     */
    @Test(timeout = 300000L)
    public void My() throws Exception {
        // SQL query to run
        String sql = "SELECT l_orderkey, o_orderstatus, c_name FROM \"lineitem\",\"customer\", \"orders\""
                + " WHERE l_orderkey = o_orderkey"
                + " AND o_custkey = c_custkey AND l_shipmode = 'RAIL'";
        String sql2 = "SELECT * FROM \"lineitem\"";

        //Customer
        //final String CUSTOMER = "customer";
        final List<String> cust_columns = Arrays.asList("c_custkey", "c_name", "c_address", "c_nationkey",
                "c_phone", "c_acctbal", "c_mktsegment", "c_comment");
        final List<Type> cust_types = Arrays.asList(Type.INTEGER, Type.STRING, Type.STRING,
                Type.INTEGER, Type.STRING, Type.DOUBLE, Type.STRING, Type.STRING);

        // LINEITEM
        //final String LINEITEM = "lineitem";
        final List<String> litem_columns = Arrays.asList("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
                "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
                "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment");
        final List<Type> litem_types = Arrays.asList(Type.INTEGER, Type.INTEGER, Type.INTEGER,
                Type.INTEGER, Type.INTEGER, Type.DOUBLE, Type.DOUBLE, Type.DOUBLE, Type.STRING, Type.STRING, Type.DATE, Type.DATE, Type.DATE,
                Type.STRING, Type.STRING, Type.STRING);

        // order
        final List<String> order_columns = Arrays.asList("o_orderkey", "o_custkey", "o_orderstatus",
                "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment");
        final List<Type> order_types = Arrays.asList(Type.INTEGER, Type.INTEGER, Type.STRING,
                Type.DOUBLE, Type.DATE, Type.STRING, Type.STRING, Type.INTEGER, Type.STRING);

        createTable(LINEITEM, litem_columns, litem_types);
        createTable(ORDERS, order_columns, order_types);
        createTable(CUSTOMER2, cust_columns, cust_types);

        insertRowsMy(ORDERS);
        insertRowsMy(CUSTOMER2);
        insert();
        delete();
        update();

        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("l_orderkey");
        Constant idValConst = Constants.createColumnNameConstant("o_orderkey");
        Expression exp1 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        idColumnNameConst = Constants.createColumnNameConstant("o_custkey");
        idValConst = Constants.createColumnNameConstant("c_custkey");
        Expression exp2 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        idColumnNameConst = Constants.createColumnNameConstant("l_shipmode");
        idValConst = Constants.createLiteralConstant("RAIL");
        Expression exp3 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        Expression expAnd = Expressions.createExpression(Operator.AND, exp1, exp2);
        Expression finalExp = Expressions.createExpression(Operator.AND, expAnd, exp3);

        final List<String> Projection = Arrays.asList("l_orderkey", "o_orderstatus", "c_name");
        QueryStatement st = Statements.buildQueryStatement(Projection, finalExp, LINEITEM, ORDERS, CUSTOMER2);

        RowCursor rc = qLayer.executeQuery(st).getRows();
        ResultSet rs = db.executeQuery(sql);
//        QueryStatement localSQL = Statements.buildQueryStatement(Arrays.asList("*"), null, LINEITEM);  */
                //        RowCursor rc = qLayer.executeQuery(localSQL).getRows();
                //        ResultSet rs = db.executeQuery(sql2);

//                 Constant idColumnNameConst = Constants.createColumnNameConstant("l_orderkey");
//        Constant idValConst = Constants.createLiteralConstant("13458");
//        Expression exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
//        QueryStatement q = Statements.buildQueryStatement(selectAll, exp, LINEITEM);
//
//        String sql3 = "SELECT * FROM \"lineitem\" where l_orderkey =  13458";

//        RowCursor rc = qLayer.executeQuery(q).getRows();
//        ResultSet rs = db.executeQuery(sql3);

        assertTrue("Set of returned rows is different than the set returned by derbyDB",
                ResultComparator.compareResultsSimple(rs, rc));
    }
    /*
     * END OF UNIT TESTS
     */

    private void createTable(String tableName, List<String> columns, List<Type> types)
            throws QueryExecutionException {
        CreateTableStatement st = Statements.buildCreateTableStatement(tableName, columns, types);

        qLayer.createTable(st);
        db.executeSQL(Statements.toSQL(st));
    }

    private void insertRows(String tableName) throws Exception {
        // Get rows from input file
        InsertRowsStatement insertStmnt = Utils.getInsertStatementFromFile(
                TestTableBuilder.class.getResourceAsStream(
                        "/data/" + tableName + ".tbl"), tableName);

        qLayer.executeInsertRows(insertStmnt);

        // Import data into Derby
        db.importData(tableName, new File(TestTableBuilder.class.getResource(
                "/data/" + tableName + "Derby.tbl").toURI()).getPath());
    }

    private void insertRowsMy(String tableName) throws Exception {
        for (int i = 0; i < 10; i++) {
            List<SimpleColumn> rows = TPCHData.createColumns(tableName, 10, i);
            int rowCount = TPCHData.getBaseSize(tableName);
            InsertRowsStatement insertStmnt = Utils.getInsertStatement(tableName,
                    rows,
                    rowCount);
            qLayer.executeInsertRows(insertStmnt);
            db.insertData("\"" + tableName + "\"", rows, rowCount);
        }
    }

    private void tableCompare(String tableName) throws Exception {
        // SQL query to validate
        String sql = "SELECT * FROM \"" + tableName + "\"";
        QueryStatement localSQL = Statements.buildQueryStatement(
                Arrays.asList(tableName), Arrays.asList("*"), null);

        RowCursor rc = qLayer.executeQuery(localSQL).getRows();
        ResultSet rs = db.executeQuery(sql);
        assertTrue("Set of returned rows is different than the set returned by derbyDB",
                ResultComparator.compareResultsSimple(rs, rc));
    }

    private void insert() throws Exception {
        for (int i = 0; i < 10; i++) {
            List<SimpleColumn> rows = TPCHData.createColumns(LINEITEM, 10, i);
            int rowCount = TPCHData.getBaseSize(LINEITEM);
            InsertRowsStatement insertStmnt = Utils.getInsertStatement(LINEITEM,
                    rows,
                    rowCount);
            qLayer.executeInsertRows(insertStmnt);
            db.insertData("\"" + LINEITEM + "\"", rows, rowCount);
        }
    }

    private void delete() throws Exception {
        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("l_orderkey");
        Constant idValConst = Constants.createLiteralConstant("10051");
        Expression exp = Expressions.createExpression(Operator.LT, idColumnNameConst, idValConst);
        DeleteRowsStatement st = Statements.buildDeleteRowsStatement(LINEITEM, exp);

        qLayer.executeDeleteRows(st);
        db.delete(LINEITEM, "l_orderkey<10051");
    }

    private void update() throws Exception {
        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("l_quantity");
        Constant idValConst = Constants.createLiteralConstant("10");
        Expression exp = Expressions.createExpression(Operator.LEQ, idColumnNameConst, idValConst);
        final List<String> litem_columns_update = Arrays.asList("l_quantity");
        final List<String> litem_values_update = Arrays.asList("1");
        UpdateRowsStatement st = Statements.buildUpdateRowStatement(LINEITEM, litem_columns_update, exp, litem_values_update);

        qLayer.executeUpdateRows(st);
        db.update(LINEITEM, "l_quantity=1", "l_quantity<=10");
    }
}
