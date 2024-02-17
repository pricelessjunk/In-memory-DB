/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.databaseImpl;

import dbs_project.database.Database;
import dbs_project.index.IndexLayer;
import dbs_project.index.IndexableTable;
import dbs_project.persistence.PersistenceLayer;
import dbs_project.persistenceImpl.PersistenceLayerimpl;
import dbs_project.query.QueryLayer;
import dbs_project.queryImpl.QueryLayerImpl;
import dbs_project.storage.StorageLayer;
import dbs_project.storageImpl.StorageLayerImpl;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author kaustuv
 */
public class DatabaseImpl implements Database {

    private final String filePath = "/tmp/DbFiles/";
    private StorageLayerImpl storage;
    private PersistenceLayer persistence;

    public DatabaseImpl() {
        File theDir = new File(filePath);

        if (!theDir.exists()) {
            theDir.mkdir();
        }
    }

    @Override
    public StorageLayer getStorageLayer() {
        if (storage == null) {
            return new StorageLayerImpl();
        } else {
            return storage;
        }
    }

    @Override
    public IndexLayer getIndexLayer() {
        if (storage == null) {
            return new StorageLayerImpl();
        } else {
            return storage;
        }
    }

    @Override
    public QueryLayer getQueryLayer() {
        if (persistence != null) {
            return new QueryLayerImpl(persistence);
        } else {
            return new QueryLayerImpl();
        }
    }

    @Override
    public PersistenceLayer getPersistenceLayer() {
        if (persistence == null) {
            persistence = new PersistenceLayerimpl(storage, filePath);
        }

        return persistence;
    }

    @Override
    public void shutDown() throws IOException {

        if (storage.getChangedTables().size() > 0) {
            storage.getChangedTables().clear();
        }
    }

    @Override
    public void startUp() throws IOException {
        File file = new File(filePath);
        File[] listOfFile = file.listFiles();
        List<IndexableTable> tableList = new ArrayList<>();

        for (File tempFile : listOfFile) {
            FileInputStream in = new FileInputStream(tempFile);
            ObjectInputStream ins = new ObjectInputStream(in);

            try {
                tableList.add((IndexableTable) ins.readObject());
            } catch (ClassNotFoundException ex) {
                throw new IOException();
            }
        }

        storage = new StorageLayerImpl(tableList);
    }

    @Override
    public void deleteDatabaseFiles() throws IOException {
        File file = new File(filePath);
        File[] listOfFile = file.listFiles();

        for (File tempFile : listOfFile) {
            tempFile.delete();

        }
    }

}
