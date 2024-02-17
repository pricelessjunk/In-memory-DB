package dbs_project.storageImpl;

import dbs_project.index.Index;
import static dbs_project.index.IndexType.HASH;
import static dbs_project.index.IndexType.TREE;
import dbs_project.indexImpl.IndexImpl;
import dbs_project.indexImpl.IndexMetaInfoImpl;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.Table;
import dbs_project.storage.Type;
import dbs_project.utilImpl.BPlusBoolTree;
import dbs_project.utilImpl.BPlusDoubleTree;
import dbs_project.utilImpl.BPlusIntTree;
import dbs_project.utilImpl.BPlusTree;
import dbs_project.indexImpl.HashTable;
import dbs_project.storage.ExtendedColumn;
import dbs_project.storage.TableMetaData;
import dbs_project.utilImpl.BooleanArrayList;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.collections.primitives.ArrayDoubleList;
import org.apache.commons.collections.primitives.ArrayIntList;

/**
 *
 * @author kaustuv
 */
public class ColumnImpl implements ExtendedColumn, Serializable {

    private static final long serialVersionUID = 7863262235394607247L;
    private ColumnMetaData colMetaData;
    private Object data;
    public List<Index> indexes;
    private TableMetaData srcTabMet;

    public ColumnImpl() {

    }

    public ColumnImpl(Table sourceTable, String name, Type type, int id, int currentRowCount) {
        indexes = new ArrayList<>();

        switch (type) {
            case INTEGER:
                data = new ArrayIntList();
                break;
            case DOUBLE:
                data = new ArrayDoubleList();
                break;
            case BOOLEAN:
                data = new BooleanArrayList();
                break;
            case STRING:

            case DATE:

            case OBJECT:
                data = new ArrayList();
        }

        this.colMetaData = new ColumnMetaDataImpl(sourceTable, name, type, id);

        if (sourceTable.getTableMetaData() != null) {               //Required for column creation before metadata creation

            if (currentRowCount > 0) {
                if (type == Type.INTEGER) {
                    for (int i = 0; i < currentRowCount; i++) {
                        ((ArrayIntList) data).add(Type.NULL_VALUE_INTEGER);
                    }
                } else if (type == Type.DOUBLE) {
                    for (int i = 0; i < currentRowCount; i++) {
                        ((ArrayDoubleList) data).add(Type.NULL_VALUE_DOUBLE);
                    }
                } else if (type == Type.BOOLEAN) {
                    for (int i = 0; i < currentRowCount; i++) {
                        ((BooleanArrayList) data).add(Type.NULL_VALUE_BOOLEAN);
                    }
                } else {
                    for (int i = 0; i < currentRowCount; i++) {
                        ((ArrayList) data).add(null);
                    }
                }

                srcTabMet = sourceTable.getTableMetaData();

            } else {
                currentRowCount = 0;
            }

            ((ColumnMetaDataImpl) colMetaData).setRowCount(currentRowCount);
        }

    }

    @Override
    public ColumnMetaData getMetaData() {
        return colMetaData;
    }

    @Override
    public int getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        int val = ((ArrayIntList) data).get(index);

        if (!(((Integer) ((ArrayIntList) data).get(index)) instanceof Integer)
                && val != Type.NULL_VALUE_INTEGER) {
            throw new ClassCastException();
        }

        //int val = this.isNull(index) ? Type.NULL_VALUE_INTEGER : ((ArrayIntList) data).get(index);
        return val;
    }

    @Override
    public boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        boolean val = ((BooleanArrayList) data).get(index);

        if (!(((Boolean) ((BooleanArrayList) data).get(index)) instanceof Boolean)
                && val != Type.NULL_VALUE_BOOLEAN) {
            throw new ClassCastException();
        }

        // boolean val = this.isNull(index) ? Type.NULL_VALUE_BOOLEAN : ((BooleanArrayList) data).get(index);
        return val;
    }

    @Override
    public double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        double val = ((ArrayDoubleList) data).get(index);

        if (!(((Double) ((ArrayDoubleList) data).get(index)) instanceof Double)
                && val != Type.NULL_VALUE_DOUBLE) {
            throw new ClassCastException();
        }

        //double val = this.isNull(index) ? Type.NULL_VALUE_DOUBLE : ((ArrayDoubleList) data).get(index);
        return val;
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (!(((ArrayList) data).get(index) instanceof Date) && ((ArrayList) data).get(index) != null) {
            throw new ClassCastException();
        }

        return (Date) ((ArrayList) data).get(index);
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        if (!(((ArrayList) data).get(index) instanceof String) && ((ArrayList) data).get(index) != null) {
            throw new ClassCastException();
        }

        return (String) ((ArrayList) data).get(index);
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        switch (colMetaData.getType()) {
            case INTEGER:
                return ((ArrayIntList) data).get(index);
            case DOUBLE:
                return ((ArrayDoubleList) data).get(index);
            case BOOLEAN:
                return ((BooleanArrayList) data).get(index);
        }

        return ((ArrayList) data).get(index);

    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        switch (colMetaData.getType()) {
            case INTEGER:
                return ((ArrayIntList) data).get(index) == Type.NULL_VALUE_INTEGER;
            case DOUBLE:
                return ((ArrayDoubleList) data).get(index) == Type.NULL_VALUE_DOUBLE;
            case BOOLEAN:
                return ((BooleanArrayList) data).get(index) == Type.NULL_VALUE_BOOLEAN;
        }

        return (((ArrayList) data).get(index) == null);
    }

    @Override
    public void addValue(Object o) throws ParseException {
        int rowCount;
        if (o instanceof Integer) {
            int val = (int) o;
            boolean b;
            ((ArrayIntList) data).add(val);

            for (Index index : indexes) {
                switch (index.getIndexMetaInfo().getIndexType()) {
                    case HASH:
                        IndexImpl indh = (IndexImpl) index;
                        b = ((HashTable) indh.getStructure()).add(val, ((ArrayIntList) data).size());
                        if (!b) {
                            ((IndexMetaInfoImpl) indh.getIndexMetaInfo()).incrementKeyCount();
                        }
                        break;
                    case TREE: {
                        IndexImpl ind = (IndexImpl) index;
                        boolean isDuplicate = ((BPlusIntTree) ind.getStructure()).add(val, ((ArrayIntList) data).size());
                        if (!isDuplicate) {
                            ((IndexMetaInfoImpl) ind.getIndexMetaInfo()).incrementKeyCount();
                        }
                    }
                }
            }
        } else if (o instanceof Double) {
            double val = (double) o;
            boolean b;

            ((ArrayDoubleList) data).add(val);

            for (Index index : indexes) {
                switch (index.getIndexMetaInfo().getIndexType()) {
                    case HASH:
                        IndexImpl indh = (IndexImpl) index;
                        b = ((HashTable) indh.getStructure()).add(val, ((ArrayDoubleList) data).size());
                        if (!b) {
                            ((IndexMetaInfoImpl) indh.getIndexMetaInfo()).incrementKeyCount();
                        }
                        break;
                    case TREE: {
                        IndexImpl ind = (IndexImpl) index;

                        boolean isDuplicate = ((BPlusDoubleTree) ind.getStructure()).add(val, ((ArrayDoubleList) data).size());
                        if (!isDuplicate) {
                            ((IndexMetaInfoImpl) ind.getIndexMetaInfo()).incrementKeyCount();
                        }
                    }
                }
            }
        } else if (o instanceof Boolean) {
            boolean val = (boolean) o;
            ((BooleanArrayList) data).add(val);
            boolean b;

            for (Index index : indexes) {
                switch (index.getIndexMetaInfo().getIndexType()) {
                    case HASH:
                        IndexImpl indh = (IndexImpl) index;
                        b = ((HashTable) indh.getStructure()).add(val, ((BooleanArrayList) data).size());
                        if (!b) {
                            ((IndexMetaInfoImpl) indh.getIndexMetaInfo()).incrementKeyCount();
                        }
                        break;
                    case TREE: {
                        IndexImpl ind = (IndexImpl) index;
                        boolean isDuplicate = ((BPlusBoolTree) ind.getStructure()).add(val ? 1 : 0, ((BooleanArrayList) data).size());
                        if (!isDuplicate) {
                            ((IndexMetaInfoImpl) ind.getIndexMetaInfo()).incrementKeyCount();
                        }
                    }
                }
            }
        } else {
            if (o instanceof String) {
                ((ArrayList) data).add((String) o);
            } else if (o instanceof Date) {
                ((ArrayList) data).add((Date) o);
            } else {
                ((ArrayList) data).add(o);
            }
            for (Index index : indexes) {
                switch (index.getIndexMetaInfo().getIndexType()) {
                    case HASH:
                        IndexImpl indh = (IndexImpl) index;
                        boolean isNotDuplicate = ((HashTable) indh.getStructure()).add(o, ((ArrayList) data).size());
                        if (!isNotDuplicate) {
                            ((IndexMetaInfoImpl) indh.getIndexMetaInfo()).incrementKeyCount();
                        }
                        break;
                    case TREE: {
                        IndexImpl ind = (IndexImpl) index;
                        boolean isDuplicate = ((BPlusTree) ind.getStructure()).add(o, ((ArrayList) data).size());
                        if (!isDuplicate) {
                            ((IndexMetaInfoImpl) ind.getIndexMetaInfo()).incrementKeyCount();
                        }
                    }
                }
            }
        }
        ((ColumnMetaDataImpl) colMetaData).incrementRowCount();
        rowCount = ((ColumnMetaDataImpl) colMetaData).getRowCount();
        ((TableMetaDataImpl) srcTabMet).setBSet(rowCount - 1);
    }

    @Override
    public void updateValue(int rowId, Object o) throws ParseException {
        if (o instanceof Integer) {
            int element = ((ArrayIntList) data).get(rowId - 1);
            ((ArrayIntList) data).set(rowId - 1, (int) o);

            for (Index index : indexes) {
                switch (index.getIndexMetaInfo().getIndexType()) {
                    case HASH:
                        IndexImpl indh = (IndexImpl) index;
                        ((HashTable) indh.getStructure()).remove(element, rowId);
                        ((HashTable) indh.getStructure()).add(o, rowId);
                        break;
                    case TREE: {
                        IndexImpl ind = (IndexImpl) index;
                        ((BPlusIntTree) ind.getStructure()).remove(element, rowId);
                        ((BPlusIntTree) ind.getStructure()).add((int) o, rowId);
                    }
                }
            }
        } else if (o instanceof Double) {
            double element = ((ArrayDoubleList) data).get(rowId - 1);
            ((ArrayDoubleList) data).set(rowId - 1, (double) o);
            for (Index index : indexes) {
                switch (index.getIndexMetaInfo().getIndexType()) {
                    case HASH:
                        IndexImpl indh = (IndexImpl) index;
                        ((HashTable) indh.getStructure()).remove(element, rowId);
                        ((HashTable) indh.getStructure()).add(o, rowId);
                        break;
                    case TREE: {
                        IndexImpl ind = (IndexImpl) index;
                        ((BPlusDoubleTree) ind.getStructure()).remove(element, rowId);
                        ((BPlusDoubleTree) ind.getStructure()).add((double) o, rowId);
                    }
                }
            }
        } else if (o instanceof String) {
            String element = (String) (((ArrayList) data).get(rowId - 1));
            ((ArrayList) data).set(rowId - 1, (String) o);
            for (Index index : indexes) {
                switch (index.getIndexMetaInfo().getIndexType()) {
                    case HASH:
                        IndexImpl indh = (IndexImpl) index;
                        ((HashTable) indh.getStructure()).remove(element, rowId);
                        ((HashTable) indh.getStructure()).add(o, rowId);
                        break;
                    case TREE: {
                        IndexImpl ind = (IndexImpl) index;
                        ((BPlusTree) ind.getStructure()).remove(element, rowId);
                        ((BPlusTree) ind.getStructure()).add(o, rowId);
                    }
                }
            }

        } else if (o instanceof Date) {
            Date elem = (Date) (((ArrayList) data).get(rowId - 1));
            String element = null;
            if (elem != null) {
                element = elem.toString();
            }
            ((ArrayList) data).set(rowId - 1, (Date) o);
            for (Index index : indexes) {
                switch (index.getIndexMetaInfo().getIndexType()) {
                    case HASH:
                        IndexImpl indh = (IndexImpl) index;
                        ((HashTable) indh.getStructure()).remove(element, rowId);
                        ((HashTable) indh.getStructure()).add(o, rowId);
                        break;
                    case TREE: {
                        IndexImpl ind = (IndexImpl) index;
                        ((BPlusTree) ind.getStructure()).remove(elem, rowId);
                        ((BPlusTree) ind.getStructure()).add(o, rowId);
                    }

                }
            }
        } else if (o instanceof Boolean) {
            Boolean element = (((BooleanArrayList) data).get(rowId - 1));
            ((BooleanArrayList) data).set(rowId - 1, (boolean) o);
            for (Index index : indexes) {
                switch (index.getIndexMetaInfo().getIndexType()) {
                    case HASH:
                        IndexImpl indh = (IndexImpl) index;
                        ((HashTable) indh.getStructure()).remove(element, rowId);
                        ((HashTable) indh.getStructure()).add(o, rowId);
                        break;
                    case TREE: {
                        IndexImpl ind = (IndexImpl) index;
                        ((BPlusBoolTree) ind.getStructure()).remove(element ? 1 : 0, rowId);
                        ((BPlusBoolTree) ind.getStructure()).add((boolean) o ? 1 : 0, rowId);
                    }
                }
            }
        } else {
            ((ArrayList) data).set(rowId - 1, o);
        }

    }

    @Override
    public void removeValue(int rowId) throws ParseException {
        switch (colMetaData.getType()) {
            case INTEGER: {
                int element = ((ArrayIntList) data).get(rowId - 1);
                ((ArrayIntList) data).set(rowId - 1, Type.NULL_VALUE_INTEGER);
                for (Index index : indexes) {
                    switch (index.getIndexMetaInfo().getIndexType()) {
                        case HASH:
                            IndexImpl indh = (IndexImpl) index;
                            ((HashTable) indh.getStructure()).remove(element, rowId);
                            ((IndexMetaInfoImpl) indh.getIndexMetaInfo()).decrementKeyCount();
                            break;
                        case TREE: {
                            IndexImpl ind = (IndexImpl) index;
                            boolean remove = ((BPlusIntTree) ind.getStructure()).remove(element, rowId);
                            if (remove) {
                                ((IndexMetaInfoImpl) ind.getIndexMetaInfo()).decrementKeyCount();
                            }
                        }
                    }
                }
            }
            break;

            case DOUBLE: {
                double element = ((ArrayDoubleList) data).get(rowId - 1);
                ((ArrayDoubleList) data).set(rowId - 1, Type.NULL_VALUE_DOUBLE);
                for (Index index : indexes) {
                    switch (index.getIndexMetaInfo().getIndexType()) {
                        case HASH:
                            IndexImpl indh = (IndexImpl) index;
                            ((HashTable) indh.getStructure()).remove(element, rowId);
                            ((IndexMetaInfoImpl) indh.getIndexMetaInfo()).decrementKeyCount();
                            break;
                        case TREE: {
                            IndexImpl ind = (IndexImpl) index;
                            boolean remove = ((BPlusDoubleTree) ind.getStructure()).remove(element, rowId);
                            if (remove) {
                                ((IndexMetaInfoImpl) ind.getIndexMetaInfo()).decrementKeyCount();
                            }
                        }

                    }
                }
            }
            break;
            case BOOLEAN: {
                boolean element = (((BooleanArrayList) data).get(rowId - 1));
                ((BooleanArrayList) data).set(rowId - 1, Type.NULL_VALUE_BOOLEAN);
                for (Index index : indexes) {
                    switch (index.getIndexMetaInfo().getIndexType()) {
                        case HASH:
                            IndexImpl indh = (IndexImpl) index;
                            ((HashTable) indh.getStructure()).remove(element, rowId);
                            ((IndexMetaInfoImpl) indh.getIndexMetaInfo()).decrementKeyCount();
                            break;
                        case TREE: {
                            IndexImpl ind = (IndexImpl) index;
                            boolean remove = ((BPlusBoolTree) ind.getStructure()).remove(element ? 1 : 0, rowId);
                            if (remove) {
                                ((IndexMetaInfoImpl) ind.getIndexMetaInfo()).decrementKeyCount();
                            }
                        }

                    }
                }
            }
            break;
            default:
                Object eleObj = ((ArrayList) data).get(rowId - 1);
                String element = null;
                if (eleObj != null) {
                    element = eleObj.toString();
                }

                ((ArrayList) data).set(rowId - 1, null);
                for (Index index : indexes) {
                    switch (index.getIndexMetaInfo().getIndexType()) {
                        case HASH:
                            IndexImpl indh = (IndexImpl) index;
                            ((HashTable) indh.getStructure()).remove(element, rowId);
                            ((IndexMetaInfoImpl) indh.getIndexMetaInfo()).decrementKeyCount();
                            break;
                        case TREE: {
                            IndexImpl ind = (IndexImpl) index;
                            boolean remove = ((BPlusTree) ind.getStructure()).remove(eleObj, rowId);
                            if (remove) {
                                ((IndexMetaInfoImpl) ind.getIndexMetaInfo()).decrementKeyCount();
                            }
                        }

                    }
                }
        }
        ((TableMetaDataImpl) srcTabMet).unsetBSet(rowId - 1);
    }

    @Override
    public Object getData() {
        return data;
    }

    @Override
    public TableMetaData getSrcTabMet() {
        return srcTabMet;
    }

    @Override
    public void setSrcTabMet(TableMetaData srcTabMet) {
        this.srcTabMet = srcTabMet;
    }

    @Override
    public List<Index> getIndexes() {
        if (indexes == null) {
            return new ArrayList<>();
        } else {
            return indexes;
        }
    }

}
