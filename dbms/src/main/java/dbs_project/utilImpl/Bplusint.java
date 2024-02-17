/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.utilImpl;

import dbs_project.storage.Type;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author kaustuv
 */
public class Bplusint {

    public static void main(String[] args) throws InterruptedException, ParseException {
        try {
            //dateTest();
            doubleTest();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void intTest() {
        System.out.println("Running Int");

        BPlusIntTree btree = new BPlusIntTree();

        for (int i = 0; i <= 49; i++) {
            btree.add(i, i);
        }

        for (int j = 30; j <= 49; j++) {
            btree.remove(j, j);
        }

        btree.remove(25, 25);

        System.out.println(btree.search(35));
        System.out.println(btree.search(48));

        System.out.println(btree.getRangedValues(28, 60, true, true));
    }

    public static void doubleTest() {
        System.out.println("Running double");

        BPlusDoubleTree btree = new BPlusDoubleTree();
        List<Double> ll = new ArrayList<>();
        List<Double> ll2 = new ArrayList<>();
        int low=990;
        int low2=603;

        for (int i = 0; i < 1000; i++) {
            /*if (i >= low && i <= 994) {
                ll.add((double)i);
            }*/
            
            if (i == low || i == low+2||i == low+4||i == low+8) {
                ll.add((double)i);
            }
            
//            if (i >= low2 && i <= 996) {
//                ll2.add((double)i);
//            }

            btree.add((double)i, i);
        }

        for (int j = 0; j < ll.size(); j++) {
            System.out.println("Removing " + ll.get(j));
            btree.remove(ll.get(j), ll.get(j).intValue());
            //System.out.println(btree.getAllValues());
            System.out.println("--------------------");
        }
        
//         for (int j = 0; j < ll2.size(); j++) {
//            System.out.println("Removing " + ll2.get(j));
//            btree.remove(ll2.get(j), j+low2);
//            System.out.println("--------------------");
//        }
        
//        System.out.println(btree.search(4));
//        System.out.println(btree.search(602));

        System.out.println(btree.getRangedValues(989, 999, true, true));
        //System.out.println(btree.getAllValues());
    }

    public static void StringTest() throws ParseException {
        System.out.println("Running String");
        BPlusTree btree = new BPlusTree(Type.STRING);
        List<Date> ll = new ArrayList<>();

        char a = 'a';

        for (int i = 0; i < 1000; i++) {
//            if (i == 930) {
//                td = c.getTime();
//            }
//
//            if (i == 948) {
//                td2 = c.getTime();
//            }
//
//            if (i >= 950 && i <= 1000) {
//                ll.add(c.getTime());
//            }
//
//            if (i == 945 || i == 947) {
//                btree.add(null, i);
//                c.add(Calendar.DATE, 1);
//                continue;
//            }

            btree.add(String.valueOf(a), i);
            a++;
        }

        for (int j = 0; j < ll.size(); j++) {
            btree.remove(ll.get(j), j + 950);
        }

        btree.remove("c", 18);

        System.out.println(btree.search("f"));
        System.out.println(btree.search("i"));
        System.out.println(btree.getRangedValues("f", "i", true, true));
        //System.out.println(btree.getRangedValues(0, 34, true, true));
    }

    public static void dateTest() throws ParseException {
        System.out.println("Running Date");
        BPlusTree btree = new BPlusTree(Type.DATE);

        Date d = new Date();

        System.out.println("Date chosen: " + d.toString());
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        Date td = null;
        Date td2 = null;
        List<Date> ll = new ArrayList<>();
        List<Date> ll2 = new ArrayList<>();
        int up=600;
        int low=500;

        for (int i = 0; i < 1000; i++) {
            if (i == 498) {
                td = c.getTime();
            }

            if (i == 999) {
                td2 = c.getTime();
            }
            
            if (i >= low && i <= up) {
                ll.add(c.getTime());
            }
            
            if (i >= 603 && i <= 996) {
                ll2.add(c.getTime());
            }

            btree.add(c.getTime(), i);
            c.add(Calendar.DATE, 1);
        }

        for (int j = 0; j < ll.size(); j++) {
            System.out.println("Removing " + ll.get(j));
            btree.remove(ll.get(j), j+low);
            //System.out.println(btree.getAllValues());
            System.out.println("--------------------");
        }
        
         for (int j = 0; j < ll2.size(); j++) {
            System.out.println("Removing " + ll2.get(j));
            btree.remove(ll2.get(j), j+603);
            System.out.println("--------------------");
        }
        
        System.out.println(btree.search(td));
        System.out.println(btree.search(td2));

        System.out.println(btree.getRangedValues(td, td2, true, true));
        
    }

}
