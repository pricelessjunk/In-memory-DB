/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.databaseImpl;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;

/**
 *
 * @author kaustuv
 */
public class testPersistance {

    static class Inner implements Serializable {

        private static final long serialVersionUID = 7863262235394607247L;
        int id;
        String name;
        Date d;

        public Inner(int id, String name, Date d) {
            this.id = id;
            this.name = name;
            this.d = d;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Date getD() {
            return d;
        }

        public void setD(Date d) {
            this.d = d;
        }

    }

    public static void main(String[] args) {
        try {
            Inner inn = new Inner(1, "Kaustuv", new Date());
//            storeToDisk(inn);
//
//            Inner i = getFromDisk();
//            System.out.println("id: " + i.getId());
//            System.out.println("Name: " + i.getName());
//            System.out.println("Date: " + i.getD());

//            deleteFiles();
            write(inn, "/tmp/DbFiles/kooFile.xml");

            Inner i = read("/tmp/DbFiles/kooFile.xml");
            System.out.println("id: " + i.getId());
            System.out.println("Name: " + i.getName());
            System.out.println("Date: " + i.getD());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void storeToDisk(Inner inn) throws FileNotFoundException, IOException {
        FileOutputStream out = new FileOutputStream("/home/kaustuv/DbFiles/kooFile");
        ObjectOutputStream os = new ObjectOutputStream(out);

        os.writeObject("foo");
        os.writeObject(inn);
        os.flush();
    }

    static Inner getFromDisk() throws FileNotFoundException, IOException, ClassNotFoundException {
        FileInputStream in = new FileInputStream("/home/kaustuv/DbFiles/kooFile");
        ObjectInputStream ins = new ObjectInputStream(in);
        String fooString = (String) ins.readObject();

        Inner inner = (Inner) ins.readObject();

        return inner;
    }

    static void deleteFiles() {
        File file = new File("/home/kaustuv/DbFiles");
        File[] listOfFile = file.listFiles();

        for (File tempFile : listOfFile) {
            tempFile.delete();

        }
    }

    public static void write(Inner inn, String filename) throws Exception {
        XMLEncoder encoder = new XMLEncoder(new BufferedOutputStream(new FileOutputStream(filename)));
        encoder.writeObject(inn);
        encoder.close();
    }

    public static Inner read(String filename) throws Exception {
        XMLDecoder decoder = new XMLDecoder(new BufferedInputStream(new FileInputStream(filename)));
        Inner o = (Inner) decoder.readObject();
        decoder.close();
        return o;
    }
}
