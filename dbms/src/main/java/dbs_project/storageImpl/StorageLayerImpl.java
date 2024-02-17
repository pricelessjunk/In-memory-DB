package dbs_project.storageImpl;

import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.TableAlreadyExistsException;
import dbs_project.index.IndexLayer;
import dbs_project.index.IndexableTable;
import dbs_project.storage.Table;
import dbs_project.storage.TableMetaData;
import dbs_project.storage.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 * @author kaustuv
 */
public class StorageLayerImpl implements IndexLayer {

    Map<Integer, IndexableTable> tableList;
    Map<String, IndexableTable> changedTables;

    public StorageLayerImpl() {
        tableList = new HashMap();
        changedTables = new HashMap();
    }

    public StorageLayerImpl(List<IndexableTable> list) {
        tableList = new HashMap();
        changedTables = new HashMap();
        for (IndexableTable t : list) {
            tableList.put(t.getTableMetaData().getId(), t);
        }
    }

    public Map<String, IndexableTable> getChangedTables() {
        return changedTables;
    }

    public void setChangedTables(Map<String, IndexableTable> changedTables) {
        this.changedTables = changedTables;
    }

    public void replaceTable(IndexableTable table) {
        if (tableList.get(table.getTableMetaData().getId()) != null) {
            tableList.put(table.getTableMetaData().getId(), table);
        }
    }

    @Override
    public int createTable(String tableName, Map<String, Type> schema) throws TableAlreadyExistsException {
        for (Table table : tableList.values()) {
            if (table.getTableMetaData().getName().equals(tableName)) {
                throw new TableAlreadyExistsException("The table " + tableName + " already Exists");
            }
        }

        Random random = new Random();
        int tableId;

        while (true) {
            tableId = random.nextInt();

            if (tableList.get(tableId) == null) {
                break;
            }
        }

        tableList.put(tableId, new TableImpl(tableId, tableName, schema));

        return tableId;
    }

    @Override
    public void deleteTable(int tableId) throws NoSuchTableException {
        if (tableList.get(tableId) == null) {
            throw new NoSuchTableException("The table does not Exists");
        }

        tableList.remove(tableId);
    }

    @Override
    public void renameTable(int tableId, String newName) throws TableAlreadyExistsException, NoSuchTableException {
        Table t = tableList.get(tableId);
        if (t == null) {
            throw new NoSuchTableException("The table does not Exists");
        }

        for (Table table : tableList.values()) {
            if (table.getTableMetaData().getName().equals(newName)) {
                throw new TableAlreadyExistsException("The table " + newName + " already Exists");
            }
        }

        TableMetaDataImpl tmd = (TableMetaDataImpl) t.getTableMetaData();
        tmd.setTableName(newName);
    }

    @Override
    public Map<String, TableMetaData> getDatabaseSchema() {
        Map<String, TableMetaData> schema = new HashMap<>();
        Table t;

        Iterator it = tableList.entrySet().iterator();
        while (it.hasNext()) {
            t = (Table) ((Map.Entry) it.next()).getValue();

            schema.put(t.getTableMetaData().getName(), t.getTableMetaData());
        }

        return schema;
    }

    @Override
    public IndexableTable getTable(int tableId) throws NoSuchTableException {
        IndexableTable t1 = tableList.get(tableId);

        if (t1 == null) {
            throw new NoSuchTableException("The table does not Exists");
        }

        return t1;
    }

    @Override
    public Collection<IndexableTable> getIndexableTables() {
        return new ArrayList<>(tableList.values());
    }

    @Override
    public Collection<Table> getTables() {
        List<Table> tables = new ArrayList<>();

        for (Map.Entry entry : tableList.entrySet()) {
            tables.add((Table) entry.getValue());
        }

        return tables;
    }
}
