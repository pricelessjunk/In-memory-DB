package dbs_project.storageImpl;

import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.Table;
import dbs_project.storage.TableMetaData;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Map;

/**
 *
 * @author kaustuv
 */
public class TableMetaDataImpl implements TableMetaData, Serializable {

    private static final long serialVersionUID = 7863262235394607247L;
    private int tableUid;
    private String tableName;
    private Map<String, ColumnMetaData> schema;
    private int rowCount;
    private BitSet deleteList;

    public TableMetaDataImpl() {

    }

    public TableMetaDataImpl(Table table, int tableId, String tableName, Map<String, ColumnMetaData> schema) {
        this.tableUid = tableId;
        this.tableName = tableName;
        this.schema = schema;
        this.rowCount = 0;
        deleteList = new BitSet();
    }

    @Override
    public Map<String, ColumnMetaData> getTableSchema() {
        return schema;
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    @Override
    public int getId() {
        return tableUid;
    }

    @Override
    public String getName() {
        return tableName;
    }

    public void setTableUid(int tableUid) {
        this.tableUid = tableUid;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void incrementRowCoutByOne() {
        rowCount++;
    }

    public void decrementRowCoutByOne() {
        rowCount--;
    }

    public void addRowCountBy(int count) {
        rowCount += count;
    }

    public void removeRowCountBy(int count) {
        rowCount -= count;
    }

    public BitSet getDeleteList() {
        return deleteList;
    }

    public void setDeleteList(BitSet deleteList) {
        this.deleteList = deleteList;
    }

    public void setBSet(int index) {
        deleteList.set(index);
    }

    public void unsetBSet(int index) {
        deleteList.set(index, false);
    }

}
