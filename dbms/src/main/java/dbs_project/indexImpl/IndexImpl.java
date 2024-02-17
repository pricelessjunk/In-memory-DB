package dbs_project.indexImpl;

import dbs_project.exceptions.InvalidKeyException;
import dbs_project.exceptions.InvalidRangeException;
import dbs_project.exceptions.RangeQueryNotSupportedException;
import dbs_project.index.Index;
import dbs_project.index.IndexMetaInfo;
import dbs_project.index.IndexType;
import dbs_project.index.IndexableTable;
import dbs_project.storage.Column;
import dbs_project.storage.ExtendedColumn;
import dbs_project.storage.RowCursor;
import dbs_project.storage.Type;
import dbs_project.storageImpl.ColumnImpl;
import dbs_project.storageImpl.TableImpl;
import dbs_project.storageImpl.TableMetaDataImpl;
import dbs_project.util.IdCursor;
import dbs_project.utilImpl.BPlusBoolTree;
import dbs_project.utilImpl.BPlusDoubleTree;
import dbs_project.utilImpl.BPlusIntTree;
import dbs_project.utilImpl.BPlusTree;
import dbs_project.utilImpl.BooleanArrayList;
import dbs_project.utilImpl.IdCursorImpl;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.collections.primitives.ArrayDoubleList;
import org.apache.commons.collections.primitives.ArrayIntList;

/**
 *
 * @author media
 */
public class IndexImpl implements Index {

    private IndexMetaInfo indexMetaData;
    private Object structure;
    private Type type;

    public IndexImpl(String indexName, ExtendedColumn keyColumn, IndexableTable table, IndexType indexType, int id) throws ParseException {
        this.indexMetaData = new IndexMetaInfoImpl(indexName, keyColumn, table, indexType, id);
        this.type = keyColumn.getMetaData().getType();
        int incCounter;
        switch (indexType) {
            case HASH:
                switch (keyColumn.getMetaData().getType()) {
                    case INTEGER:
                        structure = new HashTable();
                        incCounter = ((HashTable) structure).bLoadInt(keyColumn, ((TableMetaDataImpl) table.getTableMetaData()).getDeleteList());
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(incCounter);
                        break;
                    case DOUBLE:
                        structure = new HashTable();
                        incCounter = ((HashTable) structure).bLoadDouble(keyColumn, ((TableMetaDataImpl) table.getTableMetaData()).getDeleteList());
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(incCounter);
                        break;
                    case BOOLEAN:
                        structure = new HashTable();
                        incCounter = ((HashTable) structure).bLoadBoolean(keyColumn, ((TableMetaDataImpl) table.getTableMetaData()).getDeleteList());
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(incCounter);
                        break;
                    case STRING:
                        structure = new HashTable();
                        incCounter = ((HashTable) structure).bLoadString(keyColumn, ((TableMetaDataImpl) table.getTableMetaData()).getDeleteList());
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(incCounter);
                        break;
                    case DATE:
                        structure = new HashTable();
                        incCounter = ((HashTable) structure).bLoadDate(keyColumn, ((TableMetaDataImpl) table.getTableMetaData()).getDeleteList());
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(incCounter);
                        break;
                    case OBJECT:
                        structure = new HashTable();
                        //((HashTable)structure).bLoad((ArrayIntList) ((ColumnImpl) keyColumn).getData());
                        break;
                }
                break;
            case TREE:
                switch (keyColumn.getMetaData().getType()) {
                    case INTEGER:
                        structure = new BPlusIntTree();
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(((BPlusIntTree) structure).bulkLoad((ArrayIntList) keyColumn.getData()));
                        break;
                    case DOUBLE:
                        structure = new BPlusDoubleTree();
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(((BPlusDoubleTree) structure).bulkLoad((ArrayDoubleList) keyColumn.getData()));
                        break;
                    case BOOLEAN:
                        structure = new BPlusBoolTree();
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(((BPlusBoolTree) structure).bulkLoad((BooleanArrayList) keyColumn.getData()));
                        break;
                    case STRING:
                        structure = new BPlusTree(Type.STRING);
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(((BPlusTree) structure).bulkLoad((ArrayList) keyColumn.getData()));
                        break;
                    case DATE:
                        structure = new BPlusTree(Type.DATE);
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(((BPlusTree) structure).bulkLoad((ArrayList) keyColumn.getData()));
                        break;
                    case OBJECT:
                        structure = new BPlusTree();
                        ((IndexMetaInfoImpl) indexMetaData).AddKeyCount(((BPlusTree) structure).bulkLoad((ArrayList) keyColumn.getData()));
                }

        }

    }

    @Override
    public RowCursor pointQuery(Object searchKey) throws InvalidKeyException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public RowCursor rangeQuery(Object startSearchKey, Object endSearchKey, boolean includeStartKey, boolean includeEndKey) throws InvalidRangeException, InvalidKeyException, RangeQueryNotSupportedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public IdCursor pointQueryRowIds(Object searchKey) throws InvalidKeyException {
        try {
            switch (this.getIndexMetaInfo().getIndexType()) {
                case HASH:
                    switch (type) {
                        case INTEGER:
                            return new IdCursorImpl(((HashTable) structure).get((int) searchKey));
                        case DOUBLE:
                            return new IdCursorImpl(((HashTable) structure).get((double) searchKey));
                        case BOOLEAN:
                            return new IdCursorImpl(((HashTable) structure).get((boolean) searchKey));
                        case STRING:
                            return new IdCursorImpl(((HashTable) structure).get((String) searchKey));
                        case DATE:
                            return new IdCursorImpl(((HashTable) structure).get((Date) searchKey));
                        case OBJECT:
                    }
                case TREE:
                    switch (type) {
                        case INTEGER:
                            return new IdCursorImpl(((BPlusIntTree) structure).search((int) searchKey));
                        case DOUBLE:
                            return new IdCursorImpl(((BPlusDoubleTree) structure).search((double) searchKey));
                        case BOOLEAN:
                            return new IdCursorImpl(((BPlusBoolTree) structure).search(((boolean) searchKey) ? 1 : 0));
                        case STRING:

                        case DATE:

                        case OBJECT:
                            String key = null;
                            return new IdCursorImpl(((BPlusTree) structure).search(searchKey));
                    }

            }

            return null;
        } catch (ParseException ex) {
            throw new InvalidKeyException();
        }
    }

    @Override
    public IdCursor rangeQueryRowIds(Object startSearchKey, Object endSearchKey, boolean includeStartKey, boolean includeEndKey) throws InvalidRangeException, InvalidKeyException, RangeQueryNotSupportedException {
        ArrayIntList ids = new ArrayIntList();

        switch (type) {
            case INTEGER: {
                int start;
                int end;

                try {
                    start = (int) startSearchKey;
                    end = (int) endSearchKey;
                } catch (Exception ex) {
                    throw new InvalidKeyException();
                }

                if (end < start) {
                    throw new InvalidRangeException();
                }

                ids = ((BPlusIntTree) structure).getRangedValues(start, end, includeStartKey, includeEndKey);
                break;
            }
            case DOUBLE: {
                double start;
                double end;

                try {
                    start = (double) startSearchKey;
                    end = (double) endSearchKey;
                } catch (Exception ex) {
                    throw new InvalidKeyException();
                }

                if (end < start) {
                    throw new InvalidRangeException();
                }

                ids = ((BPlusDoubleTree) structure).getRangedValues(start, end, includeStartKey, includeEndKey);
                break;
            }
            case BOOLEAN: {
                int start;
                int end;

                try {
                    start = (boolean) startSearchKey ? 1 : 0;
                    end = (boolean) endSearchKey ? 1 : 0;
                } catch (Exception ex) {
                    throw new InvalidKeyException();
                }

                if (end < start) {
                    throw new InvalidRangeException();
                }

                ids = ((BPlusBoolTree) structure).getRangedValues(start, end, includeStartKey, includeEndKey);
                break;
            }
            case STRING:

            case DATE:

            case OBJECT: {
                try {
                    ids = ((BPlusTree) structure).getRangedValues(startSearchKey, endSearchKey, includeStartKey, includeEndKey);
                    break;
                } catch (ParseException ex) {
                    throw new InvalidKeyException();
                }
            }
        }

        return new IdCursorImpl(ids);
    }

    @Override
    public IndexMetaInfo getIndexMetaInfo() {
        return indexMetaData;
    }

    public Object getStructure() {
        return structure;
    }

}
