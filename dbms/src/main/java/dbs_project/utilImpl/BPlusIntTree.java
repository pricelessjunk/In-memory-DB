package dbs_project.utilImpl;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.primitives.ArrayIntList;

/**
 *
 * @author kaustuv
 */
public class BPlusIntTree {

    private Node globalRootNode;
    private static final int N = 4;
    private Node parent = null;

    public class Node {

        public int numKeys = 0;
        public int[] keys;
        public ArrayIntList[] values;
        public BPlusIntTree.Node[] childNodes;
        public boolean isLeafNode;
        public BPlusIntTree.Node nextNode;

        public Node() {
            numKeys = 0;
            keys = new int[2 * N - 1];
            values = new ArrayIntList[2 * N - 1];
            childNodes = new Node[2 * N];
        }
    }

    public BPlusIntTree() {
        globalRootNode = new Node();
        globalRootNode.isLeafNode = true;
    }

    public boolean add(int key, int object) {
        ArrayIntList keyVals = search(key);

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
            insertIntoNonFullNode(newRootNode, key, object);
        } else {
            insertIntoNonFullNode(rootNode, key, object);
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
            node.keys[j] = 0;
            /*if (node.values[j] == null) {
             node.values[j] = new LinkedList();
             }
             node.values[j].add(-1, -1);*/
            if (node.values[j] == null) {
                node.values[j] = new ArrayIntList();
            }
            //node.values[j].add(-1);
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
    void insertIntoNonFullNode(Node node, int key, int object) {
        int i = node.numKeys - 1;
        if (node.isLeafNode) {
            // Since node is not a full node insert the new element into its proper place within node.
            while (i >= 0 && key < node.keys[i]) {
                node.keys[i + 1] = node.keys[i];
                node.values[i + 1] = node.values[i];
                i--;
            }
            i++;
            if (key == node.keys[i] && node.numKeys > 0) {
                node.values[i].add(object);
            } else {
                node.keys[i] = key;
                /*if (node.values[i] == null) {
                 node.values[i] = new LinkedList();
                 }
                 node.values[i].add(key, object);*/
                node.values[i] = new ArrayIntList();
                node.values[i].add(object);
                node.numKeys++;
            }
        } else {
            // Move back from the last key of node until we find the child pointer to the node
            // that is the root node of the subtree where the new element should be placed.
            while (i >= 0 && key < node.keys[i]) {
                i--;
            }
            i++;
            if (node.childNodes[i].numKeys == (2 * N - 1)) {
                splitChildNode(node, i, node.childNodes[i]);
                if (key > node.keys[i]) {
                    i++;
                }
            }
            insertIntoNonFullNode(node.childNodes[i], key, object);
        }
    }

    public ArrayIntList search(int key) {
        return search(globalRootNode, key);
    }

    public ArrayIntList search(Node node, int key) {
        while (node != null) {
            int i = 0;
            while (i < node.numKeys && key > node.keys[i]) {
                i++;
            }

            if (i < node.numKeys && key == node.keys[i]) {
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

    public boolean isElementPresent(int key) {
        Node node = globalRootNode;
        while (node != null) {
            int i = 0;
            while (i < node.numKeys && key > node.keys[i]) {
                i++;
            }
            if (i < node.numKeys && key == node.keys[i]) {
                //if (node.values[i].rowIdsOf(key, true).size() > 0) {
                return true;
                //}
            }
            if (node.isLeafNode) {
                return false;
            } else {
                node = node.childNodes[i];
            }
        }
        return false;
    }

    public String getAllValues() {
        String string = "";
        Node node = globalRootNode;
        while (!node.isLeafNode) {
            node = node.childNodes[0];
        }
        while (node != null) {
            for (int i = 0; i < node.numKeys; i++) {
                string += node.values[i] + ", ";
            }
            node = node.nextNode;
        }
        return string;
    }

    public ArrayIntList getRangedValues(int fromKey, int toKey, boolean inclStartKey, boolean inclEndKey) {
        ArrayIntList ids = new ArrayIntList();
        int key = getNextGreaterKey(fromKey, inclStartKey);
        Node node = getLeafNodeForKey(key);
        parent = null;   //not needed
        while (node != null) {
            for (int j = 0; j < node.numKeys; j++) {
                if (node.keys[j] < key) {
                    continue;
                }

                if (node.keys[j] > toKey) {
                    return ids;
                } else if (node.keys[j] == toKey) {
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

    /*public Node getLeafNodeForKey(int key) {
     Node node = globalRootNode;
     while (node != null) {
     int i = 0;
     while (i < node.numKeys && key > node.keys[i]) {
     i++;
     }
     if (i < node.numKeys && key < node.keys[i]) {
     node = node.childNodes[i + 1];
     while (!node.isLeafNode) {
     node = node.childNodes[0];
     }
     return node;
     }
     if (node.isLeafNode) {
     return null;
     } else {
     node = node.childNodes[i];
     }
     }
     return null;
     }*/
    /*public Node getLeafNodeForKey(int key) {
     Node node = globalRootNode;
     parent = globalRootNode;
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
    public Node getLeafNodeForKey(int key) {
        Node node = globalRootNode;
        while (node != null) {
            int i = 0;
            while (i < node.numKeys && key > node.keys[i]) {
                i++;
            }
            if (i < node.numKeys && key == node.keys[i]) {
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

    public int bulkLoad(ArrayIntList entries) {
        int count = 0;
        for (int i = 0; i < entries.size(); i++) {
            //System.out.println(i);
            if (!add(entries.get(i), i + 1)) {
                count++;
            }
        }

        return count;
    }

    public int getNextGreaterKey(int key, boolean inclKey) {
        //Node node = globalRootNode;
        Node node = getLeafNodeForKey(key);
        parent = null; //Not needed

        if (inclKey) {
            if (key > node.keys[node.numKeys - 1]) {
                node = node.nextNode;
                return node.keys[0];
            }
        } else {
            if (key >= node.keys[node.numKeys - 1]) {
                node = node.nextNode;
                return node.keys[0];
            }
        }

        for (int i = 0; i < node.numKeys; i++) {
            if (key < node.keys[i]) {
                return node.keys[i];
            } else if (node.keys[i] == key) {
                if (inclKey) {
                    return node.keys[i];
                }
            }
        }

        return -1;
    }

    public boolean remove(int key, int rowId) {
        Node node = getLeafNodeForKey(key);
        List<Node> nodesChanged = new ArrayList<>();

        if (node != null) {
            for (int i = 0; i < node.numKeys; i++) {
                if (node.keys[i] == key) {
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

    public List<Node> getParentsList(Node leafNode) {
        List<Node> listOfParents = new ArrayList<>();
        int key = leafNode.keys[0];

        Node node = globalRootNode;

        while (node != null) {
            int i = 0;
            if (!node.isLeafNode) {
                while (i < node.numKeys && key >= node.keys[i]) {
                    i++;
                }
            }

            /*if (i < node.numKeys && key < node.keys[i]) {
             node = node.childNodes[i];
             while (!node.isLeafNode) {
             node = node.childNodes[0];
             }
             return node;
             }*/
            if (node.isLeafNode) {
                return listOfParents;
            } else {
                listOfParents.add(node);
                node = node.childNodes[i];
            }
        }
        return listOfParents;
    }

}
