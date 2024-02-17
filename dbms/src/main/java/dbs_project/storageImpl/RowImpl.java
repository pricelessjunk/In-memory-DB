package dbs_project.storageImpl;

import dbs_project.storage.Column;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.ExtendedColumn;
import dbs_project.storage.Row;
import dbs_project.storage.RowMetaData;
import dbs_project.storage.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 *
 * @author kaustuv
 */
public class RowImpl implements Row {

    //Map<Integer, Object> data;
    Object[] data;
    private RowMetaData rMetaData;

    public RowImpl() {

    }

    public RowImpl(int rowId, ColumnMetaData[] colsMDatas, List<ExtendedColumn> listOfColumns, int columnCount) {
        rMetaData = new RowMetaDataImpl(rowId, colsMDatas, columnCount);
        data = new Object[listOfColumns.size()];

        for (int i = 0; i < listOfColumns.size(); i++) {
            Column c = listOfColumns.get(i);

            switch (c.getMetaData().getType()) {
                case INTEGER:
                    data[i] = (Integer) c.getInteger(rowId - 1);
                    break;
                case DOUBLE:
                    data[i] = (Double) c.getDouble(rowId - 1);
                    break;
                case STRING:
                    data[i] = (String) c.getString(rowId - 1);
                    break;
                case DATE:
                    data[i] = (Date) c.getDate(rowId - 1);
                    break;
                case BOOLEAN:
                    data[i] = (Boolean) c.getBoolean(rowId - 1);
                    break;
                case OBJECT:
                    data[i] = (Object) c.getObject(rowId - 1);
            }

        }
    }

    public RowImpl(Object[] data, int rowId, ColumnMetaData[] colsMDatas) {
        this.data = data;
        rMetaData = new RowMetaDataImpl(rowId, colsMDatas, colsMDatas.length);
    }

    @Override
    public RowMetaData getMetaData() {
        return rMetaData;
    }

    @Override
    public int getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (data[index] instanceof Boolean) {
            throw new ClassCastException();
        }

        if (isNull(index)) {
            return Type.NULL_VALUE_INTEGER;
        }

        if (data[index] instanceof Double) {
            return ((Double) data[index]).intValue();
        }

        return (int) data[index];
    }

    @Override
    public boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (data[index] instanceof Integer) {
            throw new ClassCastException();
        }

        if (data[index] instanceof Double) {
            throw new ClassCastException();
        }

        if (isNull(index)) {
            return Type.NULL_VALUE_BOOLEAN;
        }

        return (boolean) data[index];
    }

    @Override
    public double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (data[index] instanceof Boolean) {
            throw new ClassCastException();
        }

        if (isNull(index)) {
            return Type.NULL_VALUE_DOUBLE;
        }

        if (data[index] instanceof Integer) {
            return (double) ((Integer) data[index]);
        }

        return (double) data[index];
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        return (Date) data[index];
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        if (data[index] instanceof Integer) {
            return Integer.toString((Integer) data[index]);
        }

        if (data[index] instanceof Double) {
            return Double.toString((Double) data[index]);
        }

        if (data[index] instanceof Boolean) {
            return ((Boolean) data[index]).toString();
        }

        if (data[index] instanceof Date) {
//            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//            df.setTimeZone(TimeZone.getTimeZone("UTC"));
            return ((Date) data[index]).toString();
        }

        return (String) data[index];
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        return (Object) data[index];
    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        if (data[index] instanceof Integer) {
            return ((int) data[index]) == Type.NULL_VALUE_INTEGER;
        }

        if (data[index] instanceof Double) {
            return ((double) data[index]) == Type.NULL_VALUE_DOUBLE;
        }

        if (data[index] instanceof Boolean) {
            return ((boolean) data[index]) == Type.NULL_VALUE_BOOLEAN;
        }

        return data[index] == null;
    }

}
