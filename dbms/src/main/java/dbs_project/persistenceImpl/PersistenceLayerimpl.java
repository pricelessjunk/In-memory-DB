package dbs_project.persistenceImpl;

import dbs_project.exceptions.NoTransactionActiveException;
import dbs_project.exceptions.TransactionAlreadyActiveException;
import dbs_project.index.IndexableTable;
import dbs_project.persistence.PersistenceLayer;
import dbs_project.storageImpl.StorageLayerImpl;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author kaustuv
 */
public class PersistenceLayerimpl implements PersistenceLayer {

    boolean enabled;
    boolean hasActiveTransaction;
    StorageLayerImpl storage;
    private final String filePath;
    private String lastTransaction;
    private boolean activateAuto;
    Logger logger = Logger.getLogger(PersistenceLayerimpl.class.getName());

    public PersistenceLayerimpl(StorageLayerImpl storage, String filePath) {
        this.enabled = false;
        this.hasActiveTransaction = false;
        this.storage = storage;
        this.filePath = filePath;
        lastTransaction = "";
        activateAuto = false;
    }

    public StorageLayerImpl getStorage() {
        return storage;
    }

    public void setStorage(StorageLayerImpl storage) {
        this.storage = storage;
    }

    public boolean isActivateAuto() {
        return activateAuto;
    }

    public void setActivateAuto(boolean activateAuto) {
        this.activateAuto = activateAuto;
    }

    public String getLastTransaction() {
        return lastTransaction;
    }

    public void setLastTransaction(String lastTransaction) {
        this.lastTransaction = lastTransaction;
    }

    @Override
    public void setPersistence(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public void beginTransaction() throws TransactionAlreadyActiveException {
        if (hasActiveTransaction) {
            throw new TransactionAlreadyActiveException();
        }

        if (activateAuto) {
            try {
                commitTransaction();
            } catch (NoTransactionActiveException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }

        hasActiveTransaction = true;
    }

    @Override
    public void commitTransaction() throws NoTransactionActiveException {
        if (!hasActiveTransaction && !activateAuto) {
            throw new NoTransactionActiveException();
        }

        if (!enabled) {
            storage.getChangedTables().clear();
            return;
        }

        try {
            for (Map.Entry entry : storage.getChangedTables().entrySet()) {
                IndexableTable t = (IndexableTable) entry.getValue();
                FileOutputStream out = new FileOutputStream(filePath + t.getTableMetaData().getName());
                ObjectOutputStream os = new ObjectOutputStream(out);

                os.writeObject(t);
                os.flush();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        storage.getChangedTables().clear();
        hasActiveTransaction = false;
        if (activateAuto) {
            activateAuto = false;
        }
    }

    @Override
    public void abortTransaction() throws NoTransactionActiveException {
        if (!hasActiveTransaction && !activateAuto) {
            throw new NoTransactionActiveException("No Transaction to abort");
        }

        try {
            File file = new File(filePath);
            File[] listOfFile = file.listFiles();

            for (File tempFile : listOfFile) {
                if (storage.getChangedTables().get(tempFile.getName()) == null) {
                    continue;
                }

                FileInputStream in = new FileInputStream(tempFile);
                ObjectInputStream ins = new ObjectInputStream(in);

                storage.replaceTable((IndexableTable) ins.readObject());
            }

            storage.getChangedTables().clear();
            hasActiveTransaction = false;
            if (activateAuto) {
                activateAuto = false;
            }
        } catch (ClassNotFoundException | IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public boolean hasActiveTransaction() {
        return hasActiveTransaction;
    }

}
