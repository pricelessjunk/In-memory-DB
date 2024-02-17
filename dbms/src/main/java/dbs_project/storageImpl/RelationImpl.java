package dbs_project.storageImpl;

import dbs_project.exceptions.InvalidKeyException;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchRowException;
import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.QueryExecutionException;
import dbs_project.index.Index;
import dbs_project.index.IndexType;
import dbs_project.indexImpl.HashTable;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.ExpressionElement;
import dbs_project.query.predicate.Operator;
import dbs_project.query.predicate.impl.Expressions;
import dbs_project.storage.ColumnCursor;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.ExtendedColumn;
import dbs_project.storage.Relation;
import dbs_project.storage.Row;
import dbs_project.storage.RowCursor;
import dbs_project.storage.StorageLayer;
import dbs_project.storage.Table;
import dbs_project.storage.TableMetaData;
import dbs_project.util.IdCursor;
import dbs_project.utilImpl.BooleanArrayList;
import dbs_project.utilImpl.IdCursorImpl;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.collections.primitives.ArrayDoubleList;
import org.apache.commons.collections.primitives.ArrayIntList;

/**
 *
 * @author kaustuv
 */
public class RelationImpl implements Relation {

    StorageLayer storage;
    List<Table> tables;
    List<String> colNames;
    ExpressionElement predicate;

    public RelationImpl(StorageLayer storage, List<String> tableNames, List<String> colNames, ExpressionElement predicate) throws NoSuchTableException {
        this.storage = storage;
        this.tables = new ArrayList<>();
        this.predicate = predicate;

        for (String name : tableNames) {
            TableMetaData tabMData = storage.getDatabaseSchema().get(name);
            if (tabMData == null) {
                throw new NoSuchTableException();
            }

            tables.add(storage.getTable(tabMData.getId()));
        }

        if (tables.size() > 1 && colNames.size() == 1 && colNames.get(0).equals("*")) {
            this.colNames = new ArrayList<>();
            for (Table table : tables) {
                this.colNames.addAll(new ArrayList(table.getTableMetaData().getTableSchema().keySet()));
            }
        } else {
            this.colNames = colNames;
        }
    }

    public RelationImpl(Table table) throws NoSuchTableException {
        this.tables = new ArrayList<>();
        tables.add(table);
    }

    @Override
    public RowCursor getRows() {
        if (tables.size() == 1) {                           //For single tables only
            if (predicate == null) {                                                    //When no predicate present
                if (colNames.size() == 1 && colNames.get(0).equals("*")) {              //When all columns are selected
                    return tables.get(0).getRows();
                } else {                                                              //When selected columns are selected
                    List<Row> rows = new ArrayList<>();
                    ColumnMetaData[] cMetadata = new ColumnMetaData[colNames.size()];
                    List<ExtendedColumn> columns = new ArrayList<>();

                    LoadColumnNamesForSingleTable(cMetadata, columns);

                    BitSet rowBitList = ((TableMetaDataImpl) tables.get(0).getTableMetaData()).getDeleteList();

                    int rowIndex = rowBitList.nextSetBit(0);
                    while (rowIndex != -1) {
                        Row row = new RowImpl(rowIndex + 1, cMetadata, columns, colNames.size());
                        rows.add(row);
                        rowIndex = rowBitList.nextSetBit(rowIndex + 1);
                    }

                    return new RowCursorImpl(rows);
                }
            } else {                                                                  //When predicate is present
                List<BitSet> resultSet = new ArrayList<>();

                try {
                    parseExpressionTree(resultSet, predicate);

                    if (resultSet.size() == 1) {
                        BitSet resultBits = resultSet.get(0);
                        int ind = resultBits.nextSetBit(0);

                        if (colNames.size() == 1 && colNames.get(0).equals("*")) {
                            ArrayIntList ids = new ArrayIntList();
                            while (ind != -1) {
                                ids.add(ind);
                                ind = resultBits.nextSetBit(ind + 1);
                            }

                            return tables.get(0).getRows(new IdCursorImpl(ids));
                        } else {                                                                //When selected columns are selected
                            List<ExtendedColumn> columns = new ArrayList<>();
                            ColumnMetaData[] cMetadata = new ColumnMetaData[colNames.size()];
                            List<Row> rows = new ArrayList<>();

                            if (ind == -1) {
                                return new RowCursorImpl(rows);
                            }

                            LoadColumnNamesForSingleTable(cMetadata, columns);

                            while (ind != -1) {
                                rows.add(new RowImpl(ind + 1, cMetadata, columns, colNames.size()));
                                ind = resultBits.nextSetBit(ind + 1);
                            }

                            return new RowCursorImpl(rows);
                        }
                    }

                } catch (NoSuchColumnException | NoSuchRowException | ParseException | InvalidKeyException ex) {
                    Logger.getLogger(RelationImpl.class.getName()).log(Level.SEVERE, null, ex);
                    ex.printStackTrace();
                }
            }
        } else if (tables.size() > 1) {                        //For moer than 1 table
            try {
                Map<String, List<Expression>> filters = new HashMap<>();
                List<Expression> joins = new ArrayList<>();

                parseJoinedExpressionTrees(filters, joins, predicate);

                AndMultipleFilters(filters);

                List<BitSet> resultsPerTable = new ArrayList<>();
                LoadResultsperTable(filters, resultsPerTable);

                Map<String, Integer> selectColumnsPerTable = new HashMap<>();
                ColumnMetaData[] colsMDatas = new ColumnMetaData[colNames.size()];
                LoadSelectColumnsPerTable(selectColumnsPerTable, colsMDatas);

                List<int[]> joinResult = LoadResultAfterJoin(joins, resultsPerTable);

                List<Row> rows = new ArrayList<>();
                generateJoinedRows(rows, joinResult, selectColumnsPerTable, colsMDatas);

                return new RowCursorImpl(rows);

            } catch (NoSuchColumnException | QueryExecutionException | ParseException | InvalidKeyException ex) {
                Logger.getLogger(RelationImpl.class.getName()).log(Level.SEVERE, null, ex);
                ex.printStackTrace();
            }
        }

        return null;
    }

    @Override
    public ColumnCursor getColumns() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void LoadColumnNamesForSingleTable(ColumnMetaData[] cMetadata, List<ExtendedColumn> columns) {
        for (int i = 0; i < colNames.size(); i++) {
            ColumnMetaData columnMData = tables.get(0).getTableMetaData().getTableSchema().get(colNames.get(i));
            try {
                columns.add((ExtendedColumn) tables.get(0).getColumn(columnMData.getId()));
                cMetadata[i] = columnMData;
            } catch (NoSuchColumnException ex) {
                Logger.getLogger(RelationImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public int parseExpressionTree(List<BitSet> resultSet, ExpressionElement curPredicate) throws NoSuchColumnException, ParseException, InvalidKeyException {
        Expression curExpr = (Expression) curPredicate;

        if (curExpr.getOperand(0) instanceof Constant) {
            ExtendedColumn column = null;
            int i = 0;

            for (; i < 2; i++) {
                Constant constant = (Constant) curExpr.getOperand(i);
                if (constant.getType() == Constant.ConstantType.COLUMN_NAME) {
                    ColumnMetaData columnMData = tables.get(0).getTableMetaData().getTableSchema().get(constant.getValue());
                    column = (ExtendedColumn) tables.get(0).getColumn(columnMData.getId());
                    break;
                }
            }

            Constant constant2;
            if (i == 0) {
                constant2 = (Constant) curExpr.getOperand(1);
            } else {
                constant2 = (Constant) curExpr.getOperand(0);
            }

            if (constant2.getType() == Constant.ConstantType.VALUE_LITERAL) {

                if (column.getIndexes().size() > 0 && curExpr.getOperator() == Operator.EQ) {                       //if index is present
                    Index index = column.getIndexes().get(0);

                    IdCursor result = null;

                    switch (column.getMetaData().getType()) {
                        case INTEGER:
                            result = index.pointQueryRowIds(Integer.parseInt(constant2.getValue()));
                            break;
                        case DOUBLE:
                            result = index.pointQueryRowIds(Double.parseDouble(constant2.getValue()));
                            break;
                        case BOOLEAN:
                            result = index.pointQueryRowIds(Boolean.parseBoolean(constant2.getValue()));
                            break;
                        case STRING:
                            result = index.pointQueryRowIds(constant2.getValue());
                            break;
                        case DATE:
                            result = index.pointQueryRowIds(new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(constant2.getValue()));
                            break;
                        case OBJECT:
                    }

                    resultSet.add(ArrayToBits(result));
                } else {
                    resultSet.add(compareToLiteral(column, constant2.getValue(), curExpr.getOperator()));
                }

            }

            return resultSet.size() - 1;

        } else {
            int[] trees = new int[curExpr.getOperandCount()];

            for (int i = 0; i < curExpr.getOperandCount(); i++) {
                trees[i] = parseExpressionTree(resultSet, curExpr.getOperand(i));
            }

            int firstIndex = resultSet.size() - curExpr.getOperandCount();
            for (int i = resultSet.size() - 1; i > firstIndex; i--) {
                if (curExpr.getOperator() == Operator.AND) {
                    resultSet.get(firstIndex).and(resultSet.get(i));
                    resultSet.remove(i);
                } else if (curExpr.getOperator() == Operator.OR) {
                    resultSet.get(firstIndex).or(resultSet.get(i));
                    resultSet.remove(i);
                }
            }

            return resultSet.size() - 1;
        }
    }

    private BitSet compareToLiteral(ExtendedColumn col, String literal, Operator operater) throws ParseException {
        BitSet vals = new BitSet();
        BitSet delList = ((TableMetaDataImpl) col.getSrcTabMet()).getDeleteList();

        if (operater == Operator.EQ) {
            switch (col.getMetaData().getType()) {
                case INTEGER: {
                    int lit = Integer.parseInt(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (lit == col.getInteger(j)) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case DOUBLE: {
                    double lit = Double.parseDouble(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (lit == col.getDouble(j)) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case BOOLEAN: {
                    boolean lit = Boolean.parseBoolean(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (lit == col.getBoolean(j)) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case STRING: {
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (literal.equals(col.getString(j))) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case DATE: {
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {
                        Date d = col.getDate(j);

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (d != null) {
                            if (literal.equals(d.toString())) {
                                vals.set(j);
                            }
                        }

                    }
                }
                break;
                case OBJECT:

            }
        } else if (operater == Operator.GT) {
            switch (col.getMetaData().getType()) {
                case INTEGER: {
                    int lit = Integer.parseInt(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (col.getInteger(j) > lit) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case DOUBLE: {
                    double lit = Double.parseDouble(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (col.getDouble(j) > lit) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case BOOLEAN:
                    break;
                case STRING:
                    break;
                case DATE: {
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {
                        Date d = col.getDate(j);
                        Date literalDate = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH).parse(literal);
                        if (!delList.get(j)) {
                            continue;
                        }

                        if (d != null) {
                            if (d.compareTo(literalDate) > 0) {
                                vals.set(j);
                            }
                        }

                    }
                }
                break;
                case OBJECT:

            }
        } else if (operater == Operator.LT) {
            switch (col.getMetaData().getType()) {
                case INTEGER: {
                    int lit = Integer.parseInt(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (col.getInteger(j) < lit) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case DOUBLE: {
                    double lit = Double.parseDouble(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (col.getDouble(j) < lit) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case BOOLEAN:
                    break;
                case STRING:
                    break;
                case DATE: {
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {
                        Date d = col.getDate(j);
                        Date literalDate = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH).parse(literal);
                        if (!delList.get(j)) {
                            continue;
                        }

                        if (d != null) {
                            if (d.compareTo(literalDate) < 0) {
                                vals.set(j);
                            }
                        }

                    }
                }
                break;
                case OBJECT:

            }
        } else if (operater == Operator.LEQ) {
            switch (col.getMetaData().getType()) {
                case INTEGER: {
                    int lit = Integer.parseInt(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (col.getInteger(j) <= lit) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case DOUBLE: {
                    double lit = Double.parseDouble(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (col.getDouble(j) <= lit) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case BOOLEAN:
                    break;
                case STRING:
                    break;
                case DATE: {
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {
                        Date d = col.getDate(j);
                        Date literalDate = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH).parse(literal);
                        if (!delList.get(j)) {
                            continue;
                        }

                        if (d != null) {
                            if (d.compareTo(literalDate) <= 0) {
                                vals.set(j);
                            }
                        }

                    }
                }
                break;
                case OBJECT:

            }
        } else if (operater == Operator.GEQ) {
            switch (col.getMetaData().getType()) {
                case INTEGER: {
                    int lit = Integer.parseInt(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (col.getInteger(j) >= lit) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case DOUBLE: {
                    double lit = Double.parseDouble(literal);
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {

                        if (!delList.get(j)) {
                            continue;
                        }

                        if (col.getDouble(j) >= lit) {
                            vals.set(j);
                        }
                    }
                }
                break;
                case BOOLEAN:
                    break;
                case STRING:
                    break;
                case DATE: {
                    for (int j = 0; j < col.getMetaData().getRowCount(); j++) {
                        Date d = col.getDate(j);
                        Date literalDate = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH).parse(literal);
                        if (!delList.get(j)) {
                            continue;
                        }

                        if (d != null) {
                            if (d.compareTo(literalDate) >= 0) {
                                vals.set(j);
                            }
                        }

                    }
                }
                break;
                case OBJECT:

            }
        }

        return vals;
    }

    private void parseJoinedExpressionTrees(Map<String, List<Expression>> filters, List<Expression> joins, ExpressionElement curPredicate)
            throws NoSuchColumnException, QueryExecutionException {
        Expression curExpr = (Expression) curPredicate;

        if (curExpr.getOperand(0) instanceof Constant) {
            ExtendedColumn column = null;
            int i = 0;
            int foundTableIndex = 0;

            for (; i < 2; i++) {
                Constant constant = (Constant) curExpr.getOperand(i);
                if (constant.getType() == Constant.ConstantType.COLUMN_NAME) {

                    //Check for label if given here. Else search through every tree referenced in the query
                    boolean isColumnFound = false;
                    int columnsFound = 0;
                    for (int j = 0; j < tables.size(); j++) {
                        ColumnMetaData columnMData = tables.get(j).getTableMetaData().getTableSchema().get(constant.getValue());

                        if (columnMData == null) {
                            continue;
                        }

                        column = (ExtendedColumn) tables.get(j).getColumn(columnMData.getId());
                        isColumnFound = true;
                        foundTableIndex = j;
                        columnsFound++;
                    }

                    if (!isColumnFound) {
                        throw new NoSuchColumnException(constant.getValue() + " was not found");
                    }

                    if (columnsFound > 1) {
                        throw new QueryExecutionException("Ambiguous column found for column " + constant.getValue());
                    }

                    break;
                }
            }

            Constant constant2;
            if (i == 0) {
                constant2 = (Constant) curExpr.getOperand(1);
            } else {
                constant2 = (Constant) curExpr.getOperand(0);
            }

            if (constant2.getType() == Constant.ConstantType.VALUE_LITERAL) {
                List<Expression> filteringTable = filters.get(tables.get(foundTableIndex).getTableMetaData().getName());

                if (filteringTable == null) {
                    filteringTable = new ArrayList<>();
                }

                filteringTable.add(curExpr);
                filters.put(tables.get(foundTableIndex).getTableMetaData().getName(), filteringTable);
            } else if (constant2.getType() == Constant.ConstantType.COLUMN_NAME) {
                joins.add(curExpr);
            }
        } else {

            for (int i = 0; i < curExpr.getOperandCount(); i++) {
                parseJoinedExpressionTrees(filters, joins, curExpr.getOperand(i));
            }
        }
    }

    private void AndMultipleFilters(Map<String, List<Expression>> filters) {
        for (Map.Entry entry : filters.entrySet()) {
            List<Expression> expr = (List<Expression>) entry.getValue();

            if (expr.size() > 1) {
                Expression[] expArr = new Expression[expr.size()];
                Expression andExpr = Expressions.createExpression(Operator.AND, expr.toArray(expArr));
                expr.clear();
                expr.add(andExpr);
            }
        }
    }

    private void LoadResultsperTable(Map<String, List<Expression>> filters, List<BitSet> resultsPerTable) throws NoSuchColumnException, ParseException, InvalidKeyException {

        for (int i = 0; i < tables.size(); i++) {
            List<Expression> filterPerTable = filters.get(tables.get(i).getTableMetaData().getName());

            if (filterPerTable == null) {
                resultsPerTable.add(((TableMetaDataImpl) tables.get(i).getTableMetaData()).getDeleteList());
                continue;
            }

            Expression expr = filterPerTable.get(0);

            ColumnMetaData columnMData = tables.get(0).getTableMetaData().getTableSchema().get(expr.getOperand(0).toString());
            ExtendedColumn column = (ExtendedColumn) tables.get(0).getColumn(columnMData.getId());

            if (column.getIndexes().size() > 0 && expr.getOperator() == Operator.EQ) {                       //if index is present
                Index index = column.getIndexes().get(0);

                IdCursor result = null;

                switch (column.getMetaData().getType()) {
                    case INTEGER:
                        result = index.pointQueryRowIds(Integer.parseInt(expr.getOperand(1).toString()));
                        break;
                    case DOUBLE:
                        result = index.pointQueryRowIds(Double.parseDouble(expr.getOperand(1).toString()));
                        break;
                    case BOOLEAN:
                        result = index.pointQueryRowIds(Boolean.parseBoolean(expr.getOperand(1).toString()));
                        break;
                    case STRING:
                        result = index.pointQueryRowIds(expr.getOperand(1).toString());
                        break;
                    case DATE:
                        result = index.pointQueryRowIds(new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(expr.getOperand(1).toString()));
                        break;
                    case OBJECT:
                }

                resultsPerTable.add(ArrayToBits(result));
            } else {
                resultsPerTable.add(compareToLiteral(column, expr.getOperand(1).toString(), expr.getOperator()));
            }
        }
    }

    private List<int[]> LoadResultAfterJoin(List<Expression> joins, List<BitSet> resultsPerTable) throws NoSuchColumnException {
        int[][] tableTracker = new int[joins.size()][2];
        List<List<int[]>> allTempRows = new ArrayList<>();

        for (int joinInd = 0; joinInd < joins.size(); joinInd++) {
            Expression expr = joins.get(joinInd);
            String[] columnNames = new String[2];
            columnNames[0] = ((Constant) expr.getOperand(0)).getValue();
            columnNames[1] = ((Constant) expr.getOperand(1)).getValue();

            //Ambiguity check resolve by labels
            ExtendedColumn[] columns = new ExtendedColumn[2];
            BitSet[] colBitSets = new BitSet[2];
            for (int tabId = 0; tabId < tables.size(); tabId++) {
                for (int i = 0; i < 2; i++) {
                    if (tables.get(tabId).getTableMetaData().getTableSchema().get(columnNames[i]) != null) {
                        colBitSets[i] = resultsPerTable.get(tabId);
                        columns[i] = (ExtendedColumn) tables.get(tabId).getColumn(tables.get(tabId).getTableMetaData().getTableSchema().get(columnNames[i]).getId());
                        tableTracker[joinInd][i] = tabId;
                    }
                }
            }

            List<int[]> joinRows = new ArrayList<>();

            switch (columns[0].getMetaData().getType()) {
                case INTEGER: {
                    ArrayIntList data1 = (ArrayIntList) columns[0].getData();
                    ArrayIntList data2 = (ArrayIntList) columns[1].getData();

                    //left with right
                    int ind1 = colBitSets[0].nextSetBit(0);
                    while (ind1 != -1) {
                        int ind2 = colBitSets[1].nextSetBit(0);
                        while (ind2 != -1) {
                            if (data1.get(ind1) == data2.get(ind2)) {
                                int[] tempRow = {ind1, ind2};
                                joinRows.add(tempRow);
                            }
                            ind2 = colBitSets[1].nextSetBit(ind2 + 1);
                        }

                        ind1 = colBitSets[0].nextSetBit(ind1 + 1);
                    }

                }
                break;
                case DOUBLE: {
                    ArrayDoubleList data1 = (ArrayDoubleList) columns[0].getData();
                    ArrayDoubleList data2 = (ArrayDoubleList) columns[1].getData();

                    //left with right
                    int ind1 = colBitSets[0].nextSetBit(0);
                    while (ind1 != -1) {
                        int ind2 = colBitSets[1].nextSetBit(0);
                        while (ind2 != -1) {
                            if (data1.get(ind1) == data2.get(ind2)) {
                                int[] tempRow = {ind1, ind2};
                                joinRows.add(tempRow);
                            }
                            ind2 = colBitSets[1].nextSetBit(ind2 + 1);
                        }

                        ind1 = colBitSets[0].nextSetBit(ind1 + 1);
                    }

                }
                break;
                case STRING: {
                    ArrayList data1 = (ArrayList) columns[0].getData();
                    ArrayList data2 = (ArrayList) columns[1].getData();

                    //left with right
                    int ind1 = colBitSets[0].nextSetBit(0);
                    while (ind1 != -1) {
                        int ind2 = colBitSets[1].nextSetBit(0);
                        while (ind2 != -1) {
                            if (data1.get(ind1) == null) {
                                if (data2.get(ind2) == null) {
                                    int[] tempRow = {ind1, ind2};
                                    joinRows.add(tempRow);
                                }
                            } else if (data1.get(ind1).equals(data2.get(ind2))) {
                                int[] tempRow = {ind1, ind2};
                                joinRows.add(tempRow);
                            }
                            ind2 = colBitSets[1].nextSetBit(ind2 + 1);
                        }

                        ind1 = colBitSets[0].nextSetBit(ind1 + 1);
                    }

                }
                break;
                case DATE: {
                    ArrayList data1 = (ArrayList) columns[0].getData();
                    ArrayList data2 = (ArrayList) columns[1].getData();

                    //left with right
                    int ind1 = colBitSets[0].nextSetBit(0);
                    while (ind1 != -1) {
                        int ind2 = colBitSets[1].nextSetBit(0);
                        while (ind2 != -1) {
                            if (data1.get(ind1) == null) {
                                if (data2.get(ind2) == null) {
                                    int[] tempRow = {ind1, ind2};
                                    joinRows.add(tempRow);
                                }
                            } else if (data2.get(ind2) == null) {
                                if (data1.get(ind1) == null) {
                                    int[] tempRow = {ind1, ind2};
                                    joinRows.add(tempRow);
                                }
                            } else if (data1.get(ind1).toString().equals(data2.get(ind2).toString())) {
                                int[] tempRow = {ind1, ind2};
                                joinRows.add(tempRow);
                            }
                            ind2 = colBitSets[1].nextSetBit(ind2 + 1);
                        }

                        ind1 = colBitSets[0].nextSetBit(ind1 + 1);
                    }

                }
                break;
                case BOOLEAN: {
                    BooleanArrayList data1 = (BooleanArrayList) columns[0].getData();
                    BooleanArrayList data2 = (BooleanArrayList) columns[1].getData();

                    //left with right
                    int ind1 = colBitSets[0].nextSetBit(0);
                    while (ind1 != -1) {
                        int ind2 = colBitSets[1].nextSetBit(0);
                        while (ind2 != -1) {
                            if (data1.get(ind1) == data2.get(ind2)) {
                                int[] tempRow = {ind1, ind2};
                                joinRows.add(tempRow);
                            }
                            ind2 = colBitSets[1].nextSetBit(ind2 + 1);
                        }

                        ind1 = colBitSets[0].nextSetBit(ind1 + 1);
                    }

                }
                break;
                case OBJECT:
                //col.addValue(rows.getObject(i));
                }

            allTempRows.add(joinRows);
        }

        List<int[]> finalRowList = new ArrayList<>();

        for (int i = 0; i < allTempRows.get(0).size(); i++) {
            int[] firstList = allTempRows.get(0).get(i);

            for (int j = 0; j < allTempRows.get(1).size(); j++) {
                int[] secondList = allTempRows.get(1).get(j);

                if (firstList[1] == secondList[0]) {
                    int[] finalList = {firstList[0], firstList[1], secondList[1]};
                    finalRowList.add(finalList);
                }
            }
        }

        return finalRowList;
    }

    private void LoadSelectColumnsPerTable(Map<String, Integer> selectColumnsPerTable, ColumnMetaData[] colsMDatas) throws NoSuchColumnException {
        for (int tabId = 0; tabId < tables.size(); tabId++) {
            for (int i = 0; i < colNames.size(); i++) {
                ColumnMetaData colMData = tables.get(tabId).getTableMetaData().getTableSchema().get(colNames.get(i));
                if (colMData != null) {
                    selectColumnsPerTable.put(colMData.getName(), tabId);
                    colsMDatas[i] = colMData;
                }
            }
        }
    }

    private void generateJoinedRows(List<Row> rows, List<int[]> joinResult, Map<String, Integer> selectColumnsPerTable, ColumnMetaData[] colsMDatas) throws NoSuchColumnException {
        int rowId = 0;

        for (int joinId = 0; joinId < joinResult.size(); joinId++) {
            int[] result = joinResult.get(joinId);
            Object[] data = new Object[colNames.size()];
            int position = 0;

            for (String colName : colNames) {
                int corresTable = selectColumnsPerTable.get(colName);
                ExtendedColumn col = (ExtendedColumn) tables.get(corresTable).getColumn(tables.get(corresTable).getTableMetaData().getTableSchema().get(colName).getId());

                switch (col.getMetaData().getType()) {
                    case INTEGER:
                        data[position] = col.getInteger(result[corresTable]);
                        break;
                    case DOUBLE:
                        data[position] = col.getDouble(result[corresTable]);
                        break;
                    case STRING:
                        data[position] = col.getString(result[corresTable]);
                        break;
                    case DATE:
                        data[position] = col.getDate(result[corresTable]);
                        break;
                    case BOOLEAN:
                        data[position] = col.getBoolean(result[corresTable]);
                        break;
                    case OBJECT:
                        data[position] = col.getObject(result[corresTable]);
                }
                position++;
            }

            rows.add(new RowImpl(data, rowId, colsMDatas));
            rowId++;

        }
    }

    private BitSet ArrayToBits(IdCursor result) {
        BitSet bSet = new BitSet();

        while (result.next()) {
            bSet.set(result.getId() - 1);
        }

        return bSet;
    }

}
