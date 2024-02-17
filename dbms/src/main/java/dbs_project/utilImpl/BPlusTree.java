package dbs_project.utilImpl;

import dbs_project.storage.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.commons.collections.primitives.ArrayIntList;

/**
 *
 * @author kaustuv
 */
public class BPlusTree {

    private Node globalRootNode;
    private static final int N = 4;
    private Type type;
    SimpleDateFormat df = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
    ArrayIntList nullList = new ArrayIntList();

    public class Node {

        public int numKeys = 0;
        public Object[] keys;
        public ArrayIntList[] values;
        public BPlusTree.Node[] childNodes;
        public boolean isLeafNode;
        public BPlusTree.Node nextNode;

        public Node() {
            numKeys = 0;
            keys = new Object[2 * N - 1];
            values = new ArrayIntList[2 * N - 1];
            childNodes = new Node[2 * N];
        }
    }

    public BPlusTree() {

    }

    public BPlusTree(Type type) {
        globalRootNode = new Node();
        globalRootNode.isLeafNode = true;
        this.type = type;
    }

    public boolean add(Object objKey, int object) throws ParseException {

        if (objKey == null) {
            nullList.add(object);

            if (nullList.size() > 1) {
                return true;
            } else {
                return false;
            }
        }

        ArrayIntList keyVals = search(objKey);

        if (keyVals.size() > 0) {
            keyVals.add(object);
            return true;
        }

        Node rootNode = globalRootNode;
        if (rootNode.numKeys == (2 * N - 1)) {
            Node newRootNode = new Node();
            globalRootNode = newRootNode;
            newRootNode.isLeafNode = false;
            globalRootNode.childNodes[0] = rootNode;
            splitChildNode(newRootNode, 0, rootNode);
            insertIntoNonFullNode(newRootNode, objKey, object);
        } else {
            insertIntoNonFullNode(rootNode, objKey, object);
        }
        return (keyVals.size() > 0);
    }

    void splitChildNode(Node parentNode, int i, Node node) {
        Node newNode = new Node();
        newNode.isLeafNode = node.isLeafNode;
        newNode.numKeys = N;
        for (int j = 0; j < N; j++) { // Copy the last T elements of node into newNode. Keep the median key as duplicate in the first key of newNode.
            newNode.keys[j] = node.keys[j + N - 1];
            newNode.values[j] = node.values[j + N - 1];
        }

        if (!newNode.isLeafNode) {
            for (int j = 0; j < N + 1; j++) { // Copy the last T + 1 pointers of node into newNode.
                newNode.childNodes[j] = node.childNodes[j + N - 1];
            }
            for (int j = N; j <= node.numKeys; j++) {
                node.childNodes[j] = null;
            }
        } else {
            // Manage the linked list that is used e.g. for doing fast range queries.
            newNode.nextNode = node.nextNode;
            node.nextNode = newNode;
        }

        for (int j = N - 1; j < node.numKeys; j++) {
            node.keys[j] = "";
            if (node.values[j] == null) {
                node.values[j] = new ArrayIntList();
            }
        }

        node.numKeys = N - 1;

        for (int j = parentNode.numKeys; j >= i + 1; j--) {
            parentNode.childNodes[j + 1] = parentNode.childNodes[j];
        }

        parentNode.childNodes[i + 1] = newNode;

        for (int j = parentNode.numKeys - 1; j >= i; j--) {
            parentNode.keys[j + 1] = parentNode.keys[j];
            parentNode.values[j + 1] = parentNode.values[j];
        }

        parentNode.keys[i] = newNode.keys[0];
        parentNode.values[i] = newNode.values[0];
        parentNode.numKeys++;
    }

    // Insert an element into a B-Tree. (The element will ultimately be inserted into a leaf node).	
    void insertIntoNonFullNode(Node node, Object key, int object) throws ParseException {
        int i = node.numKeys - 1;
        if (node.isLeafNode) {
            // Since node is not a full node insert the new element into its proper place within node.
            while (i >= 0 && compareValues(key, node.keys[i]) < 0) {
                node.keys[i + 1] = node.keys[i];
                node.values[i + 1] = node.values[i];
                i--;
            }
            i++;
            if (key.equals(node.keys[i]) && node.numKeys > 0) {
                node.values[i].add(object);
            } else {
                node.keys[i] = key;
                node.values[i] = new ArrayIntList();
                node.values[i].add(object);
                node.numKeys++;
            }
        } else {
            // Move back from the last key of node until we find the child pointer to the node
            // that is the root node of the subtree where the new element should be placed.
            while (i >= 0 && compareValues(key, node.keys[i]) < 0) {
                i--;
            }
            i++;
            if (node.childNodes[i].numKeys == (2 * N - 1)) {
                splitChildNode(node, i, node.childNodes[i]);
                if (compareValues(key, node.keys[i]) > 0) {
                    i++;
                }
            }
            insertIntoNonFullNode(node.childNodes[i], key, object);
        }
    }

    public ArrayIntList search(Node node, Object key) throws ParseException {
        while (node != null) {
            int i = 0;
            while (i < node.numKeys && compareValues(key, node.keys[i]) > 0) {
                i++;
            }
            if (i < node.numKeys && key.equals(node.keys[i])) {
                if (node.isLeafNode) {
                    return node.values[i];//.rowIdsOf(key, false);
                }
                i++;
            }
            if (node.isLeafNode) {
                return new ArrayIntList();
            } else {
                node = node.childNodes[i];
            }
        }
        return new ArrayIntList();
    }

    public ArrayIntList search(Object key) throws ParseException {
        if (key == null) {
            return nullList;
        }

        return search(globalRootNode, key);
    }

    public String getAllValues() {
        Node node = globalRootNode;
        while (!node.isLeafNode) {
            node = node.childNodes[0];
        }

        StringBuilder string = new StringBuilder();
        while (node != null) {
            for (int i = 0; i < node.numKeys; i++) {
                string.append(node.values[i]).append(", ");
            }
            string.append("\n");
            node = node.nextNode;
        }
        return string.toString();
    }

    public ArrayIntList getRangedValues(Object fromKey, Object toKey, boolean inclStartKey, boolean inclEndKey) throws ParseException {
        ArrayIntList ids = new ArrayIntList();

        Object key = getNextGreaterKey(fromKey, inclStartKey);
        Node node = getLeafNodeForKey(key);
        while (node != null) {
            for (int j = 0; j < node.numKeys; j++) {
                if (compareValues(key, node.keys[j]) > 0) {
                    continue;
                }

                if (compareValues(node.keys[j], toKey) > 0) {
                    return ids;
                } else if (node.keys[j].equals(toKey)) {
                    if (inclEndKey) {
                        ids.addAll(node.values[j]);
                    }
                    return ids;
                } else {
                    ids.addAll(node.values[j]);
                }

            }
            node = node.nextNode;
        }
        return ids;
    }

    /*public Node getLeafNodeForKey(long key) {
     Node node = globalRootNode;
     while (node != null) {
     int i = 0;
     if (!node.isLeafNode) {
     while (i < node.numKeys && key >= node.keys[i]) {
     i++;
     }
     }

     if (i < node.numKeys && key < node.keys[i]) {
     node = node.childNodes[i];
     while (!node.isLeafNode) {
     node = node.childNodes[0];
     }
     return node;
     }

     if (node.isLeafNode) {
     return node;
     } else {
     node = node.childNodes[i];
     }
     }
     return null;
     }*/
    public Node getLeafNodeForKey(Object key) throws ParseException {
        Node node = globalRootNode;
        while (node != null) {
            int i = 0;
            while (i < node.numKeys && compareValues(key, node.keys[i]) > 0) {
                i++;
            }
            if (i < node.numKeys && key.equals(node.keys[i])) {
                if (node.isLeafNode) {
                    return node;
                }
                i++;
            }
            if (node.isLeafNode) {
                return node;
            } else {
                node = node.childNodes[i];
            }
        }
        return null;
    }

    public int bulkLoad(ArrayList entries) throws ParseException {
        int count = 0;
        for (int i = 0; i < entries.size(); i++) {
            if (!add(entries.get(i), i + 1)) {
                count++;
            }
        }

        return count;

    }

    private long getCRC(Object key) {
        byte[] bytes = null;

        if (key == null) {
            return -1;
        }

        if (key instanceof String) {
            String keyStr = (String) key;
            bytes = keyStr.getBytes();
        } else if (key instanceof Date) {
            Date keyDate = (Date) key;
            bytes = keyDate.toString().getBytes();
        }

        Checksum crc = new CRC32();
        crc.update(bytes, 0, bytes.length);
        return crc.getValue();

    }

    public Object getNextGreaterKey(Object key, boolean inclKey) throws ParseException {
        Node node = getLeafNodeForKey(key);

        if (inclKey) {
            if (compareValues(key, node.keys[node.numKeys - 1]) > 0) {
                node = node.nextNode;
                return node.keys[0];
            }
        } else {
            if (compareValues(key, node.keys[node.numKeys - 1]) >= 0) {
                node = node.nextNode;
                return node.keys[0];
            }
        }

        for (int i = 0; i < node.numKeys; i++) {
            if (compareValues(key, node.keys[i]) < 0) {
                return node.keys[i];
            } else if (node.keys[i].equals(key)) {
                if (inclKey) {
                    return node.keys[i];
                }
            }
        }

        return null;
    }

    public boolean remove(Object objKey, int rowId) throws ParseException {
        /*String key = null;
         if (objKey == null) {
         key = "null";
         } else if (objKey instanceof Date) {
         key = objKey.toString();
         } else if (objKey instanceof String) {
         key = (String) objKey;
         }*/

        Node node = getLeafNodeForKey(objKey);
        List<Node> nodesChanged = new ArrayList<>();

        if (node != null) {
            for (int i = 0; i < node.numKeys; i++) {
                if (node.keys[i].equals(objKey)) {
                    node.values[i].removeElement(rowId);

                    if (node.values[i].size() > 0) {
                        return false;
                    }

                    for (int j = i; j < node.numKeys - 1; j++) {
                        node.keys[j] = node.keys[j + 1];
                        node.values[j] = node.values[j + 1];
                    }

                    node.numKeys--;

                    nodesChanged.add(node);
                    /*if (node.keys.length < N) {
                     restructure = true;
                     }*/

                    while (node.numKeys < N) {
                        Node nextNode = node.nextNode;

                        if (nextNode == null) {
                            break;
                        }

                        int curLength = node.numKeys;
                        int l = curLength;
                        for (; l <= N; l++) {
                            if (l - curLength > nextNode.numKeys) {
                                break;
                            }

                            node.keys[l] = nextNode.keys[l - curLength];
                            node.values[l] = nextNode.values[l - curLength];
                            node.numKeys++;
                            nextNode.numKeys--;
                        }

                        nodesChanged.add(nextNode);

                        if (nextNode.numKeys == 0) {        //if the nextnode becomes empty
                            node.nextNode = null;
                        } else {
                            int m = 0;
                            for (; m < nextNode.numKeys; m++) {
                                nextNode.keys[m] = nextNode.keys[l - curLength + m];
                                nextNode.values[m] = nextNode.values[l - curLength + m];
                            }

                            node = nextNode;
                        }
                    }

                    break;
                }
            }

            for (Node n : nodesChanged) {
                List<Node> listOfParents = getParentsList(n);
                Node child = n;

                for (int k = listOfParents.size() - 1; k >= 0; k--) {
                    Node parent = listOfParents.get(k);

                    for (int i = 0; i < parent.numKeys; i++) {
                        parent.keys[i] = parent.childNodes[i + 1].keys[0];
                    }

                    if (child.numKeys == 0 && child.nextNode == null) {
                        parent.numKeys--;
                    }

                    child = parent;
                }

            }

            return true;
        }

        return false;
    }

    public List<Node> getParentsList(Node leafNode) throws ParseException {
        List<Node> listOfParents = new ArrayList<>();
        Object key = leafNode.keys[0];

        Node node = globalRootNode;

        while (node != null) {
            int i = 0;
            if (!node.isLeafNode) {
                while (i < node.numKeys && compareValues(key, node.keys[i]) >= 0) {
                    i++;
                }
            }

            if (node.isLeafNode) {
                return listOfParents;
            } else {
                listOfParents.add(node);
                node = node.childNodes[i];
            }
        }
        return listOfParents;
    }

    public int compareValues(Object s1, Object s2) throws ParseException {
        if (type == Type.STRING) {
            return s1.toString().compareTo(s2.toString());
        } else {
            /*if (s1.equals("") && !s2.equals("")) {
             return -1;
             } else if (!s1.equals("") && s2.equals("")) {
             return 1;
             } else if (s1.equals("") && s2.equals("")) {
             return 0;
             }*/

            Date d1 = (Date) s1;
            Date d2 = (Date) s2;

            return d1.compareTo(d2);
        }

    }

}
