package dbs_project.storageImpl;

import dbs_project.storage.Column;
import dbs_project.storage.ColumnCursor;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.ExtendedColumn;
import dbs_project.storage.Type;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 *
 * @author kaustuv
 */
public class ColumnCursorImpl implements ColumnCursor {

    private Column[] columns;
    private int cursor;

    public ColumnCursorImpl() {

    }

    public ColumnCursorImpl(List<ExtendedColumn> columns) {
        this.columns = new Column[columns.size()];

        this.columns = columns.toArray(this.columns);

        cursor = -1;
    }

    @Override
    public ColumnMetaData getMetaData() {
        return columns[cursor].getMetaData();
    }

    @Override
    public int getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (columns[cursor].getMetaData().getType() == Type.DOUBLE) {
            return (int) this.getDouble(index);
        }

        return (int) columns[cursor].getInteger(index);
    }

    @Override
    public boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        return (boolean) columns[cursor].getBoolean(index);
    }

    @Override
    public double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (columns[cursor].getMetaData().getType() == Type.INTEGER) {
            return (double) this.getInteger(index);
        }

        return (double) columns[cursor].getDouble(index);
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        return (Date) columns[cursor].getDate(index);
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        Type type = columns[cursor].getMetaData().getType();

        if (isNull(index)) {
            return null;
        }

        if (type == Type.INTEGER) {
            return Integer.toString(this.getInteger(index));
        }

        if (type == Type.BOOLEAN) {
            return Boolean.toString(this.getBoolean(index));
        }

        if (type == Type.DOUBLE) {
            return Double.toString(this.getDouble(index));
        }

        if (type == Type.DATE) {
            Date d = this.getDate(index);
            return (d == null) ? null : d.toString();
        }

        return (String) columns[cursor].getString(index);
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        if (isNull(index)) {
            return null;
        }

        return (Object) columns[cursor].getObject(index);
    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        return columns[cursor].isNull(index);
    }

    @Override
    public boolean next() {
        ++cursor;
        return (cursor < columns.length);
    }

    @Override
    public void close() throws IOException {
        columns = null;
    }

}
