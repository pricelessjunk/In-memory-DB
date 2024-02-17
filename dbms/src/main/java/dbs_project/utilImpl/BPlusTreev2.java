/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.utilImpl;

import dbs_project.storage.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.collections.primitives.ArrayIntList;

/**
 *
 * @author kaustuv
 */
public class BPlusTreev2 {

    private static final int N = 4;
    private static final int splitAfter = 6;
    private static final int joinAfter = 2;
    private Node globalRootNode;
    private Type type;
    SimpleDateFormat df = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
    ArrayIntList nullList = new ArrayIntList();

    public class Node {

        public int numKeys = 0;
        public Object[] keys;
        public ArrayIntList[] values;
        public Node[] childNodes;
        public boolean isLeafNode;
        public Node nextNode;
        public Node prevNode;

        public Node() {
            numKeys = 0;
            keys = new Object[2 * N - 1];
            values = new ArrayIntList[2 * N - 1];
            childNodes = new Node[2 * N];
        }
    }

    public BPlusTreev2() {

    }

    public BPlusTreev2(Type type) {
        globalRootNode = new Node();
        globalRootNode.isLeafNode = true;
        this.type = type;
    }

    public boolean add(Object objKey, int object) throws ParseException {

        if (objKey == null) {
            nullList.add(object);

            return nullList.size() > 1;
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
//            splitChildNode(newRootNode, 0, rootNode);
            insertIntoNonFullNode(newRootNode, objKey, object);
        } else {
            insertIntoNonFullNode(rootNode, objKey, object);
        }
        return (keyVals.size() > 0);
    }

    public ArrayIntList search(Object key) throws ParseException {
        if (key == null) {
            return nullList;
        }

        return search(globalRootNode, key);
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

    public int compareValues(Object s1, Object s2) throws ParseException {
        if (type == Type.STRING) {
            return s1.toString().compareTo(s2.toString());
        } else {
            Date d1 = (Date) s1;
            Date d2 = (Date) s2;

            return d1.compareTo(d2);
        }

    }

    void insertIntoNonFullNode(Node node, Object key, int object) throws ParseException {
        int i = node.numKeys - 1;
        if (node.isLeafNode) {
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
//                splitChildNode(node, i, node.childNodes[i]);
                if (compareValues(key, node.keys[i]) > 0) {
                    i++;
                }
            }
            insertIntoNonFullNode(node.childNodes[i], key, object);
        }
    }
}
