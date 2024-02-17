package dbs_project.storageImpl;

import dbs_project.exceptions.ColumnAlreadyExistsException;
import dbs_project.exceptions.IndexAlreadyExistsException;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchIndexException;
import dbs_project.exceptions.NoSuchRowException;
import dbs_project.exceptions.SchemaMismatchException;
import dbs_project.index.Index;
import dbs_project.index.IndexType;
import dbs_project.index.IndexableTable;
import dbs_project.indexImpl.IndexImpl;
import dbs_project.storage.Column;
import dbs_project.storage.ColumnCursor;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.ExtendedColumn;
import dbs_project.storage.Row;
import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import dbs_project.storage.TableMetaData;
import dbs_project.storage.Type;
import dbs_project.util.IdCursor;
import dbs_project.utilImpl.IdCursorImpl;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.collections.primitives.ArrayIntList;

/**
 *
 * @author kaustuv
 */
public class TableImpl implements IndexableTable, Serializable {

    private static final long serialVersionUID = 7863262235394607247L;
    private Map<Integer, ExtendedColumn> columnList;
    private Map<Integer, Index> indexes;
    private TableMetaData tMetaData;

    public TableImpl() {

    }

    public TableImpl(int tableId, String tableName, Map<String, Type> schema) {
        Map<String, ColumnMetaData> cMap = new HashMap<>();
        columnList = new HashMap<>();
        indexes = new HashMap<>();
        String colName;
        Type colType;

        for (Map.Entry entry : schema.entrySet()) {
            colName = (String) entry.getKey();
            colType = (Type) entry.getValue();

            ExtendedColumn col = new ColumnImpl(this, colName, colType, columnList.size(), -1);
            cMap.put(colName, col.getMetaData());
            columnList.put(col.getMetaData().getId(), col);
        }

        tMetaData = new TableMetaDataImpl(this, tableId, tableName, cMap);

        for (Map.Entry entry : columnList.entrySet()) {
            ColumnImpl col = (ColumnImpl) entry.getValue();
            col.setSrcTabMet(tMetaData);
        }
    }

    @Override
    public void renameColumn(int columnId, String newColumnName) throws ColumnAlreadyExistsException, NoSuchColumnException {
        Column col = columnList.get(columnId);

        if (col == null) {
            throw new NoSuchColumnException("The Column with id " + columnId + " does not exist");
        }

        Column compareCol;
        for (Map.Entry entry : columnList.entrySet()) {
            compareCol = (Column) entry.getValue();

            if (newColumnName.equals(compareCol.getMetaData().getName())) {
                throw new ColumnAlreadyExistsException("Column " + newColumnName + " already Exists");
            }
        }

        tMetaData.getTableSchema().remove(col.getMetaData().getName());
        ((ColumnMetaDataImpl) col.getMetaData()).setName(newColumnName);
        tMetaData.getTableSchema().put(newColumnName, col.getMetaData());
    }

    @Override
    public int createColumn(String columnName, Type columnType) throws ColumnAlreadyExistsException {
        Column compareCol;
        int maxId = -1;
        for (Map.Entry entry : columnList.entrySet()) {
            compareCol = (Column) entry.getValue();

            if (columnName.equals(compareCol.getMetaData().getName())) {
                throw new ColumnAlreadyExistsException("Column " + columnName + " already Exists");
            }

            maxId = maxId > compareCol.getMetaData().getId() ? maxId : compareCol.getMetaData().getId();
        }

        int currentRowCount = -1;

        if (maxId != -1) {
            currentRowCount = columnList.get(maxId).getMetaData().getRowCount();
        }

        ExtendedColumn col = new ColumnImpl(this, columnName, columnType, maxId + 1, currentRowCount);
        tMetaData.getTableSchema().put(columnName, col.getMetaData());
        columnList.put(maxId + 1, col);

        return maxId + 1;
    }

    @Override
    public int addRow(Row row) throws SchemaMismatchException {
        RowMetaData rMetaData = row.getMetaData();
        for (int i = 0; i < rMetaData.getColumnCount(); i++) {
            ColumnMetaData cMetData = rMetaData.getColumnMetaData(i);
            if (tMetaData.getTableSchema().get(cMetData.getName()) == null) {
                throw new SchemaMismatchException("The Row contains incorrect data");
            }
        }

        for (int i = 0; i < row.getMetaData().getColumnCount(); i++) {
            try {
                ColumnMetaData cMetData = rMetaData.getColumnMetaData(i);
                ColumnImpl col = (ColumnImpl) columnList.get((tMetaData.getTableSchema().get(cMetData.getName())).getId());

                switch (cMetData.getType()) {
                    case INTEGER:
                        col.addValue(row.getInteger(i));
                        break;
                    case DOUBLE:
                        col.addValue(row.getDouble(i));
                        break;
                    case STRING:
                        col.addValue(row.getString(i));
                        break;
                    case DATE:
                        col.addValue(row.getDate(i));
                        break;
                    case BOOLEAN:
                        col.addValue(row.getBoolean(i));
                        break;
                    case OBJECT:
                        col.addValue(row.getObject(i));
                }
            } catch (ParseException ex) {
                Logger.getLogger(TableImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        ((TableMetaDataImpl) tMetaData).incrementRowCoutByOne();
        return rMetaData.getId();
    }

    @Override
    public IdCursor addRows(RowCursor rows) throws SchemaMismatchException {
        ArrayIntList ids = new ArrayIntList();
        ColumnMetaData cMetData = null;
        boolean checkSchema = true;

        while (rows.next()) {
            RowMetaData rMetaData = rows.getMetaData();

            if (checkSchema) {
                for (int i = 0; i < rMetaData.getColumnCount(); i++) {
                    cMetData = rMetaData.getColumnMetaData(i);
                    if (tMetaData.getTableSchema().get(cMetData.getName()) == null) {
                        throw new SchemaMismatchException("The Row contains incorrect data");
                    }
                }
                checkSchema = false;
            }

            ColumnImpl col = null;
            for (int i = 0; i < rows.getMetaData().getColumnCount(); i++) {
                try {
                    cMetData = rMetaData.getColumnMetaData(i);
                    col = (ColumnImpl) columnList.get((tMetaData.getTableSchema().get(cMetData.getName())).getId());

                    switch (cMetData.getType()) {
                        case INTEGER:
                            col.addValue(rows.getInteger(i));
                            break;
                        case DOUBLE:
                            col.addValue(rows.getDouble(i));
                            break;
                        case STRING:
                            col.addValue(rows.getString(i));
                            break;
                        case DATE:
                            col.addValue(rows.getDate(i));
                            break;
                        case BOOLEAN:
                            col.addValue(rows.getBoolean(i));
                            break;
                        case OBJECT:
                            col.addValue(rows.getObject(i));
                    }
                } catch (ParseException ex) {
                    Logger.getLogger(TableImpl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            ids.add(columnList.get(0).getMetaData().getRowCount());
            ((TableMetaDataImpl) tMetaData).incrementRowCoutByOne();
        }

        return new IdCursorImpl(ids);
    }

    @Override
    public int addColumn(Column column) throws SchemaMismatchException, ColumnAlreadyExistsException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IdCursor addColumns(ColumnCursor columns) throws SchemaMismatchException, ColumnAlreadyExistsException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteRow(int rowId) throws NoSuchRowException {
        for (Map.Entry entry : columnList.entrySet()) {
            try {
                ColumnImpl col = (ColumnImpl) entry.getValue();
                col.removeValue(rowId);
            } catch (ParseException ex) {
                Logger.getLogger(TableImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        ((TableMetaDataImpl) tMetaData).decrementRowCoutByOne();
    }

    @Override
    public void deleteRows(IdCursor rowIds) throws NoSuchRowException {
        while (rowIds.next()) {
            deleteRow(rowIds.getId());
        }

    }

    @Override
    public void dropColumn(int columnId) throws NoSuchColumnException {
        Column col = columnList.get(columnId);

        if (col == null) {
            throw new NoSuchColumnException("The Column with id " + columnId + " does not exist");
        }

        tMetaData.getTableSchema().remove(col.getMetaData().getName());
        columnList.remove(col.getMetaData().getId());

        List<Index> indList = new ArrayList<>(indexes.values());

        for (Index index : indList) {
            if (index.getIndexMetaInfo().getKeyColumn().getMetaData().getId() == columnId) {
                indexes.remove(index.getIndexMetaInfo().getId());
            }
        }
    }

    @Override
    public void dropColumns(IdCursor columnIds) throws NoSuchColumnException {
        while (columnIds.next()) {
            dropColumn(columnIds.getId());
        }
    }

    @Override
    public ExtendedColumn getColumn(int columnId) throws NoSuchColumnException {
        if (columnList.get(columnId) == null) {
            throw new NoSuchColumnException("The Column with id " + columnId + " does not exist");
        }

        return columnList.get(columnId);
    }

    @Override
    public ColumnCursor getColumns(IdCursor columnIds) throws NoSuchColumnException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RowCursor getRows(IdCursor rowIds) throws NoSuchRowException {
        /*List<Row> rows = new ArrayList<>();
         int columnCount = columnList.size();
         if (columnCount > 0) {
         List<ExtendedColumn> cols = sortColumns();
         ColumnMetaData[] colsMDatas = sortColumnMetaData();
         int rowCount = cols.get(0).getMetaData().getRowCount();

         while (rowIds.next()) {
         if (rowIds.getId() > rowCount) {
         throw new NoSuchRowException("Row does not exist");
         }
         rows.add(new RowImpl(rowIds.getId(), colsMDatas, cols, columnCount));
         }

         }
         return new RowCursorImpl(rows);*/
        BitSet idList = new BitSet();
        List<ExtendedColumn> cols = sortColumns();
        int rowCount = cols.get(0).getMetaData().getRowCount();

        while (rowIds.next()) {

            if (rowIds.getId() > rowCount) {
                throw new NoSuchRowException("Row does not exist");
            }

            idList.set(rowIds.getId() - 1);
        }
        return new RowCursorImpl(this, idList);
    }

    @Override
    public Row getRow(int rowId) throws NoSuchRowException {
        List<ExtendedColumn> cols = new ArrayList<>(columnList.values());

        if (cols.size() > 0) {
            if (rowId > cols.get(0).getMetaData().getRowCount()) {
                throw new NoSuchRowException("Row does not exist");
            }
        }

        Row row = new RowImpl(rowId, sortColumnMetaData(), sortColumns(), columnList.size());

        return row;
    }

    @Override
    public void updateRow(int rowId, Row newRow) throws SchemaMismatchException, NoSuchRowException {
        ColumnMetaData cMet = null;
        RowMetaData rMet = newRow.getMetaData();
        for (int i = 0; i < rMet.getColumnCount(); i++) {
            cMet = rMet.getColumnMetaData(i);

            if (columnList.get(tMetaData.getTableSchema().get(cMet.getName()).getId()) == null) {
                throw new SchemaMismatchException("Columns don't match the table's column List");
            }
        }

        if (columnList.size() > 0) {
            if (rowId > columnList.get(0).getMetaData().getRowCount()) {
                throw new NoSuchRowException("Row does not exist");
            }
        }

        for (int i = 0; i < rMet.getColumnCount(); i++) {

            try {
                cMet = rMet.getColumnMetaData(i);

                ColumnImpl c = (ColumnImpl) columnList.get(tMetaData.getTableSchema().get(cMet.getName()).getId());

                switch (cMet.getType()) {
                    case INTEGER:
                        c.updateValue(rowId, newRow.getInteger(i));
                        break;
                    case DOUBLE:
                        c.updateValue(rowId, newRow.getDouble(i));
                        break;
                    case STRING:
                        c.updateValue(rowId, newRow.getString(i));
                        break;
                    case DATE:
                        c.updateValue(rowId, newRow.getDate(i));
                        break;
                    case BOOLEAN:
                        c.updateValue(rowId, newRow.getBoolean(i));
                        break;
                    case OBJECT:
                        c.updateValue(rowId, newRow.getObject(i));
                }
            } catch (ParseException ex) {
                Logger.getLogger(TableImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    @Override
    public void updateRows(IdCursor rowIds, RowCursor newRows) throws SchemaMismatchException, NoSuchRowException {
        ColumnMetaData cMet = null;
        RowMetaData rMet = null;

        while (newRows.next()) {

            rMet = newRows.getMetaData();

            for (int i = 0; i < rMet.getColumnCount(); i++) {
                cMet = rMet.getColumnMetaData(i);

                if (columnList.get(tMetaData.getTableSchema().get(cMet.getName()).getId()) == null) {
                    throw new SchemaMismatchException("Columns don't match the table's column List");
                }
            }

            rowIds.next();
            int rowId = rowIds.getId();

            if (columnList.size() > 0) {
                if (rowId > columnList.get(0).getMetaData().getRowCount()) {
                    throw new NoSuchRowException("Row does not exist");
                }
            }

            for (int i = 0; i < rMet.getColumnCount(); i++) {

                try {
                    cMet = rMet.getColumnMetaData(i);

                    ColumnImpl c = (ColumnImpl) columnList.get(tMetaData.getTableSchema().get(cMet.getName()).getId());

                    switch (cMet.getType()) {
                        case INTEGER:
                            c.updateValue(rowId, newRows.getInteger(i));
                            break;
                        case DOUBLE:
                            c.updateValue(rowId, newRows.getDouble(i));
                            break;
                        case STRING:
                            c.updateValue(rowId, newRows.getString(i));
                            break;
                        case DATE:
                            c.updateValue(rowId, newRows.getDate(i));
                            break;
                        case BOOLEAN:
                            c.updateValue(rowId, newRows.getBoolean(i));
                            break;
                        case OBJECT:
                            c.updateValue(rowId, newRows.getObject(i));
                    }
                } catch (ParseException ex) {
                    Logger.getLogger(TableImpl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

    }

    @Override
    public void updateColumns(IdCursor columnIds, ColumnCursor updateColumns) throws SchemaMismatchException, NoSuchColumnException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateColumn(int columnId, Column updateColumn) throws SchemaMismatchException, NoSuchColumnException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public TableMetaData getTableMetaData() {
        return tMetaData;
    }

    @Override
    public RowCursor getRows() {
        /*int rowIndex = 0;
         List<Row> rows = new ArrayList<>();
         Row row = null;
         List<ExtendedColumn> listOfColumns = sortColumns();
         ColumnMetaData[] colsMDatas = sortColumnMetaData();
         int columnCount = columnList.size();
         BitSet delSet = ((TableMetaDataImpl) tMetaData).getDeleteList();
         for (int i = delSet.nextSetBit(0); rowIndex != -1; i = delSet.nextSetBit(i + 1)) {
         System.out.println("Hello");
         if (i != -1) {
         row = new RowImpl(i + 1, colsMDatas, listOfColumns, columnCount);
         rows.add(row);
         }
         rowIndex = delSet.nextSetBit(i + 1);
         }
         return new RowCursorImpl(rows);*/
        // System.out.println(delSet);
        return new RowCursorImpl(this, ((TableMetaDataImpl) this.getTableMetaData()).getDeleteList());
    }

    @Override
    public ColumnCursor getColumns() {
        return new ColumnCursorImpl(sortColumns());
    }

    @Override
    public int createIndex(String indexName, int keyColumnId, IndexType indexType) throws IndexAlreadyExistsException, NoSuchColumnException {

        ExtendedColumn col = columnList.get(keyColumnId);

        if (col == null) {
            throw new NoSuchColumnException("The Column with id " + keyColumnId + " does not exist");
        }

        List<Index> indList = new ArrayList(indexes.values());

        int maxId = -1;
        for (int i = 0; i < indList.size(); i++) {
            if (indList.get(i).getIndexMetaInfo().getName().equals(indexName)) {
                throw new IndexAlreadyExistsException("The index with name " + indexName + " already exists");
            }
            maxId = indList.get(i).getIndexMetaInfo().getId() > maxId ? indList.get(i).getIndexMetaInfo().getId() : maxId;
        }
        try {
            Index index = new IndexImpl(indexName, col, this, indexType, maxId + 1);
            indexes.put(maxId + 1, index);
            ((ColumnImpl) col).indexes.add(index);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }

        return maxId + 1;

    }

    @Override
    public void dropIndex(int indexId) throws NoSuchIndexException {
        Index index = indexes.get(indexId);

        if (index == null) {
            throw new NoSuchIndexException("Index " + indexId + " is not found");
        }

        indexes.remove(indexId);

        ColumnImpl col = (ColumnImpl) index.getIndexMetaInfo().getKeyColumn();
        for (int i = 0; i < col.indexes.size(); i++) {
            if (col.indexes.get(i).getIndexMetaInfo().getId() == indexId) {
                col.indexes.remove(i);
                break;
            }
        }
    }

    @Override
    public Collection<Index> getIndexes(int keyColumnId) throws NoSuchColumnException {
        if (columnList.get(keyColumnId) == null) {
            throw new NoSuchColumnException();
        }

        List<Index> iList = new ArrayList<>();
        List<Index> indList = new ArrayList<>(indexes.values());

        for (Index index : indList) {
            if (index.getIndexMetaInfo().getKeyColumn().getMetaData().getId() == keyColumnId) {
                iList.add(index);
            }
        }

        return iList;
    }

    @Override
    public Collection<Index> getIndexes() {
        return indexes.values();
    }

    @Override
    public Index getIndex(int indexId) throws NoSuchIndexException {
        if (indexes.get(indexId) == null) {
            throw new NoSuchIndexException("index id " + indexId + " not found");
        }
        return indexes.get(indexId);
    }

    public List<ExtendedColumn> sortColumns() {
        List<ExtendedColumn> listOfColumns = new ArrayList<>(columnList.values());
        Collections.sort(listOfColumns, new Comparator<Column>() {

            @Override
            public int compare(Column o1, Column o2) {
                return o1.getMetaData().getId() - o2.getMetaData().getId();
            }
        });

        return listOfColumns;
    }

    private ColumnMetaData[] sortColumnMetaData() {
        List<ColumnMetaData> listOfColumnsMdatas = new ArrayList<>(tMetaData.getTableSchema().values());
        ColumnMetaData[] colMetsArr = new ColumnMetaData[columnList.size()];
        Collections.sort(listOfColumnsMdatas, new Comparator<ColumnMetaData>() {

            @Override
            public int compare(ColumnMetaData o1, ColumnMetaData o2) {
                return o1.getId() - o2.getId();
            }
        });

        colMetsArr = listOfColumnsMdatas.toArray(colMetsArr);
        return colMetsArr;
    }
}
