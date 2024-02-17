package dbs_project.storageImpl;

import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.RowMetaData;

/**
 *
 * @author kaustuv
 */
public class RowMetaDataImpl implements RowMetaData {

    private int rowId;
    private int columnCount;
    private ColumnMetaData[] cols;

    public RowMetaDataImpl() {

    }

    public RowMetaDataImpl(int rowId, ColumnMetaData[] colsMDatas, int columnCount) {
        this.rowId = rowId;
        this.columnCount = columnCount;
        this.cols = colsMDatas;
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public ColumnMetaData getColumnMetaData(int positionInTheRow) throws IndexOutOfBoundsException {
        return cols[positionInTheRow];
    }

    @Override
    public int getId() {
        return rowId;
    }

}
