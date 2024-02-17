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
public class testBPlusTree {

    public static void main(String[] args) throws InterruptedException, ParseException {
        BPlusTree btree = new BPlusTree(Type.DATE);
//        BPlusIntTree btree = new BPlusIntTree();

        Date d = new Date();

        System.out.println("DAte chosen: " + d.toString());
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        Date td = null;
        Date td2 = null;
        char a = 'a';
        List<Date> ll = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            //System.out.println(i + "---" + c.getTime());
            if (i == 930) {
                td = c.getTime();
            }

            if (i == 948) {
                td2 = c.getTime();
            }

            if (i >= 950 && i <= 990) {
                ll.add(c.getTime());
            }

            if (i == 945 || i == 947) {
                btree.add(null, i);
                c.add(Calendar.DATE, 1);
                continue;
            }

            btree.add(c.getTime(), i);
            c.add(Calendar.DATE, 1);
            //btree.add(String.valueOf(a), i);
//                           a++;
        }

        for (int j = 0; j < ll.size(); j++) {
            btree.remove(ll.get(j), j + 950);
        }

        btree.remove(td, 18);

        System.out.println(btree.search(td));
        System.out.println(btree.search(td2));

        System.out.println(btree.getRangedValues(td, td2, true, true));
//        System.out.println(btree.search("f"));
//        System.out.println(btree.search("i"));
//        System.out.println(btree.getRangedValues("f", "i", true, true));
//        System.out.println(btree.getRangedValues(0, 34, true, true));

    }

}
