package dbs_project.indexImpl;

/**
 *
 * @author Dilip
 */
import dbs_project.storage.ExtendedColumn;
import dbs_project.utilImpl.BooleanArrayList;
import java.util.*;
import org.apache.commons.collections.primitives.ArrayIntList;

import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.commons.collections.primitives.ArrayDoubleList;

public class HashTable {

    class Node {

        private Object data;
        int rowID;

        public Node(Object data, int rowID) {
            this.data = data;
            this.rowID = rowID;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }

        public int getRowID() {
            return rowID;
        }

        public void setRowID(int rowID) {
            this.rowID = rowID;
        }
    }

    int size = 1000;
    public List[] bucketLists;
    int elementCounter;

    public HashTable() {
        bucketLists = new List[size];
        elementCounter = 0;
    }

    public boolean add(Object element, int rowID) {
        if (elementCounter >= (0.8 * bucketLists.length)) {
            rehash();
        }

        int index = getIndex(element, size);

        boolean isElementPresent = isElement(element, index);

        if (bucketLists[index] == null) {
            bucketLists[index] = new ArrayList();
        }

        if (element == null) {
            element = "NaN";
        }

        bucketLists[index].add(new Node(element, rowID));
        elementCounter++;

        return isElementPresent;
    }

    public int bLoadInt(ExtendedColumn keyColumn, BitSet delList) {
        ArrayIntList entries = (ArrayIntList) keyColumn.getData();
        int colCur = delList.nextSetBit(0);
        int x = 0;

        while (colCur != -1) {
            boolean isDuplicate = add(entries.get(colCur), colCur + 1);
            if (!isDuplicate) {
                x++;
            }
            colCur = delList.nextSetBit(colCur + 1);
        }
        return x;
    }

    public int bLoadDouble(ExtendedColumn keyColumn, BitSet delList) {
        ArrayDoubleList entries = (ArrayDoubleList) keyColumn.getData();
        int colCur = delList.nextSetBit(0);
        int x = 0;

        while (colCur != -1) {
            boolean isDuplicate = add(entries.get(colCur), colCur + 1);
            if (!isDuplicate) {
                x++;
            }
            colCur = delList.nextSetBit(colCur + 1);
        }
        return x;
    }

    public int bLoadString(ExtendedColumn keyColumn, BitSet delList) {
        ArrayList entries = (ArrayList) keyColumn.getData();
        int colCur = delList.nextSetBit(0);
        int x = 0;

        while (colCur != -1) {
            if (entries.get(colCur) != null) {
                boolean isDuplicate = add(entries.get(colCur), colCur + 1);
                if (!isDuplicate) {
                    x++;
                }
            } else {
                boolean isDuplicate = add("NaN", colCur + 1);
                if (!isDuplicate) {
                    x++;
                }
            }
            colCur = delList.nextSetBit(colCur + 1);
        }
        return x;
    }

    public int bLoadBoolean(ExtendedColumn keyColumn, BitSet delList) {
        BooleanArrayList entries = (BooleanArrayList) keyColumn.getData();
        int colCur = delList.nextSetBit(0);
        int x = 0;

        while (colCur != -1) {
            boolean isDuplicate = add(entries.get(colCur), colCur + 1);
            if (!isDuplicate) {
                x++;
            }
            colCur = delList.nextSetBit(colCur + 1);
        }
        return x;
    }

    public int bLoadDate(ExtendedColumn keyColumn, BitSet delList) {
        ArrayList entries = (ArrayList) keyColumn.getData();
        int colCur = delList.nextSetBit(0);
        int x = 0;

        while (colCur != -1) {
            if (entries.get(colCur) != null) {
                boolean isDuplicate = add(entries.get(colCur), colCur + 1);
                if (!isDuplicate) {
                    x++;
                }
            } else {
                boolean isDuplicate = add("NaN", colCur + 1);
                if (!isDuplicate) {
                    x++;
                }
            }
            colCur = delList.nextSetBit(colCur + 1);
        }
        return x;
    }

    public void remove(Object element, int rowID) {
        int index = getIndex(element, size);

        for (int i = 0; i < bucketLists[index].size(); i++) {
            Node node = (Node) bucketLists[index].get(i);

            if (node.getRowID() == rowID) {
                bucketLists[index].remove(i);
                elementCounter--;
                break;
            }
        }
    }

    public ArrayIntList get(Object element) {
        int index = getIndex(element, size);
        ArrayIntList list = new ArrayIntList();

        if (bucketLists[index] != null) {
            List bucket = bucketLists[index];

            if (element == null) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((String) ((Node) bucket.get(i)).getData()).equals("NaN")) {
                        list.add(((Node) bucket.get(i)).getRowID());
                    }
                }
            } else if (element instanceof Integer) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((Integer) ((Node) bucket.get(i)).getData()) == (int) element) {
                        list.add(((Node) bucket.get(i)).getRowID());
                    }
                }
            } else if (element instanceof Double) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((Double) ((Node) bucket.get(i)).getData()) == ((double) element)) {
                        list.add(((Node) bucket.get(i)).getRowID());
                    }
                }
            } else if (element instanceof String) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((String) ((Node) bucket.get(i)).getData()).equals((String) element)) {
                        list.add(((Node) bucket.get(i)).getRowID());
                    }
                }
            } else if (element instanceof Date) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((Date) ((Node) bucket.get(i)).getData()).equals((Date) element)) {
                        list.add(((Node) bucket.get(i)).getRowID());
                    }
                }
            } else if (element instanceof Boolean) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((Boolean) ((Node) bucket.get(i)).getData()) == ((boolean) element)) {
                        list.add(((Node) bucket.get(i)).getRowID());
                    }
                }
            }
        }

        return list;
    }

    public boolean isElement(Object element, int index) {
        List bucket = bucketLists[index];

        if (bucket != null) {
            if (element == null) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((String) ((Node) bucket.get(i)).getData()).equals("NaN")) {
                        return true;
                    }
                }
            } else if (element instanceof Integer) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((int) ((Node) bucket.get(i)).getData()) == (int) element) {
                        return true;
                    }
                }
            } else if (element instanceof Double) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((double) ((Node) bucket.get(i)).getData()) == (double) element) {
                        return true;
                    }
                }
            } else if (element instanceof String) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((String) ((Node) bucket.get(i)).getData()).equals((String) element)) {
                        return true;
                    }
                }
            } else if (element instanceof Date) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((Date) ((Node) bucket.get(i)).getData()).equals((Date) element)) {
                        return true;
                    }
                }
            } else if (element instanceof Boolean) {
                for (int i = 0; i < bucket.size(); i++) {
                    if (((boolean) ((Node) bucket.get(i)).getData()) == ((boolean) element)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public void rehash() {
        int modSize = 2 * size;
        List[] modBucketList = new List[modSize];

        for (List bucket : bucketLists) {
            if (bucket != null) {
                for (int j = 0; j < bucket.size(); j++) {
                    Node node = (Node) bucket.get(j);
                    if (node == null) {
                        continue;
                    }

                    Object element = node.getData();
                    int rowID = node.getRowID();

                    int index = getIndex(element, modSize);

                    if (modBucketList[index] == null) {
                        modBucketList[index] = new ArrayList();
                    }

                    modBucketList[index].add(new Node(element, rowID));
                }
            }
        }

        bucketLists = modBucketList;
        size = modSize;
    }

    private long getCrc(String value) {
        Checksum checksum = new CRC32();
        byte bytes[] = value.getBytes();
        checksum.update(bytes, 0, bytes.length);
        return checksum.getValue();
    }

    private int getIndex(Object element, int size) {
        int index = 0;

        if (element == null) {
            index = (int) getCrc("NaN") % size;
        } else if (element instanceof Integer) {
            index = (int) element % size;
        } else if (element instanceof Double) {
            index = (int) ((double) element % size);
        } else if (element instanceof String) {
            index = (int) (getCrc((String) element) % size);
        } else if (element instanceof Date) {
            index = (int) (getCrc(element.toString()) % size);
        } else if (element instanceof Boolean) {
            index = (boolean) element ? 1 : 0;
        }

        if (index < 0) {
            index *= -1;
        }

        return index;
    }
}
