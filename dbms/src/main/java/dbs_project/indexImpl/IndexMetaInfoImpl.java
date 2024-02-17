package dbs_project.indexImpl;

import dbs_project.index.IndexMetaInfo;
import dbs_project.index.IndexType;
import dbs_project.index.IndexableTable;
import dbs_project.storage.Column;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author media
 */
public class IndexMetaInfoImpl implements IndexMetaInfo {

    private int keycount;
    private IndexableTable table;
    private Column keyColumn;
    private int id;
    private String name;
    private IndexType indexType;

    public IndexMetaInfoImpl(String indexName, Column keyColumn, IndexableTable table, IndexType indexType, int id) {
        this.name = indexName;
        this.table = table;
        this.id = id;
        this.keyColumn = keyColumn;
        this.indexType = indexType;
        this.keycount = 0;
    }

    @Override

    public IndexableTable getTable() {
        return table;
    }

    @Override
    public Column getKeyColumn() {
        return keyColumn;
    }

    @Override
    public int getKeyCount() {
        return keycount;
    }

    @Override
    public IndexType getIndexType() {
        return indexType;
    }

    @Override
    public boolean supportsRangeQueries() {
        boolean rQueries = false;
        switch (indexType) {
            case HASH:
                rQueries = false;
            case TREE:
                rQueries = true;
        }
        return rQueries;
    }

    @Override
    public int getId() {
        Map<Object, Object> m = null;
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    public void incrementKeyCount() {
        keycount++;
    }
    
    public void decrementKeyCount() {
        keycount--;
    }
    
    public void AddKeyCount(int number) {
        keycount = keycount+number;
    }

}
