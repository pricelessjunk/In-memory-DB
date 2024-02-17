package dbs_project.storageImpl;

import dbs_project.storage.Column;
import dbs_project.storage.ColumnCursor;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.ExtendedColumn;
import dbs_project.storage.Row;
import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import dbs_project.storage.Table;
import dbs_project.storage.Type;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import org.objectweb.asm.xml.SAXAnnotationAdapter;

/**
 *
 * @author kaustuv
 */
public class RowCursorImpl implements RowCursor {

    private Row[] rows;
    private Column[] columns;
    private int cursor;
    private boolean rowMode;
    BitSet validRows;
    TableImpl tab;

    public RowCursorImpl() {
        cursor = -1;
    }

    public RowCursorImpl(List<Row> rows) {
        cursor = -1;
        rowMode = true;
        this.rows = new Row[rows.size()];
        this.rows = rows.toArray(this.rows);
    }

    public RowCursorImpl(Table table, BitSet validRows) {
        cursor = -1;
        rowMode = false;
        tab = (TableImpl) table;
        this.validRows = validRows;
        List<ExtendedColumn> cols = tab.sortColumns();
        this.columns = new Column[cols.size()];
        this.columns = cols.toArray(this.columns);
    }

    @Override
    public RowMetaData getMetaData() {
        if (rowMode) {
            return rows[cursor].getMetaData();
        } else {
            ColumnMetaData[] cMetaData = getColumnMetaDataArray();
            RowMetaData rMet = new RowMetaDataImpl(cursor + 1, cMetaData, columns.length);
            return rMet;
        }
    }

    @Override
    public int getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (rowMode) {
            return rows[cursor].getInteger(index);
        } else {
            if (columns[index].getMetaData().getType() == Type.DOUBLE) {
            return ((Double) columns[index].getDouble(cursor)).intValue();
            }
            return columns[index].getInteger(cursor);
        }
    }

    @Override
    public boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (rowMode) {
            return rows[cursor].getBoolean(index);
        } else {
            return columns[index].getBoolean(cursor);
        }
    }

    @Override
    public double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (rowMode) {
            return rows[cursor].getDouble(index);
        } else {
            if (columns[index].getMetaData().getType() == Type.INTEGER) {
                return (double) (columns[index].getInteger(cursor));
            }
            return columns[index].getDouble(cursor);
        }
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (rowMode) {
            return rows[cursor].getDate(index);
        } else {
            return columns[index].getDate(cursor);
        }
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        if (rowMode) {
            return rows[cursor].getString(index);
        } else {
            if (columns[index].getMetaData().getType() == Type.INTEGER) {
                return Integer.toString(columns[index].getInteger(cursor));
            }

            if (columns[index].getMetaData().getType() == Type.DOUBLE) {
                return Double.toString(columns[index].getDouble(cursor));
            }

            if (columns[index].getMetaData().getType() == Type.BOOLEAN) {
                return ((Boolean) columns[index].getBoolean(cursor)).toString();
            }

            if (columns[index].getMetaData().getType() == Type.DATE) {
                if(columns[index].getDate(cursor) == null){
                    return null;
                }
                return ((Date) (columns[index].getDate(cursor))).toString();
            }
            return columns[index].getString(cursor);
        }
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        if (rowMode) {
            return rows[cursor].getObject(index);
        } else {
            return columns[index].getObject(cursor);
        }
    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        if (rowMode) {
            return rows[cursor].getObject(index) == null;
        } else {
            return columns[index].getObject(cursor) == null;
        }
    }

    @Override
    public boolean next() {

        if (rowMode) {
            if (rows == null) {
                return false;
            }
            if (rows.length == 0) {
                return false;
            }
            return (++cursor < rows.length);
        } else {
            if (columns == null) {
                return false;
            } else if (columns.length == 0) {
                return false;
            } else {
                ++cursor;
                cursor = validRows.nextSetBit(cursor);
                if(cursor == -1){
                    return false;
                }else{
                    return true;
                }
            }

        }
    }

    @Override
    public void close() throws IOException {
        if (rowMode) {
            rows = null;
        } else {
            columns = null;
        }
    }

    private ColumnMetaData[] getColumnMetaDataArray() {
        List<ColumnMetaData> listOfColumnsMdatas = new ArrayList<>(tab.getTableMetaData().getTableSchema().values());
        ColumnMetaData[] colMetsArr = new ColumnMetaData[columns.length];
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
