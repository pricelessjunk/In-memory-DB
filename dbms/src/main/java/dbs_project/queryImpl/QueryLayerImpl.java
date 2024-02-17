package dbs_project.queryImpl;

import dbs_project.database.DatabaseFactory;
import dbs_project.exceptions.ColumnAlreadyExistsException;
import dbs_project.exceptions.IndexAlreadyExistsException;
import dbs_project.exceptions.InvalidKeyException;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchIndexException;
import dbs_project.exceptions.NoSuchRowException;
import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.NoTransactionActiveException;
import dbs_project.exceptions.QueryExecutionException;
import dbs_project.exceptions.TableAlreadyExistsException;
import dbs_project.exceptions.TransactionAlreadyActiveException;
import dbs_project.index.Index;
import dbs_project.index.IndexableTable;
import dbs_project.persistence.PersistenceLayer;
import dbs_project.persistenceImpl.PersistenceLayerimpl;
import dbs_project.query.QueryLayer;
import dbs_project.query.statement.CreateColumnStatement;
import dbs_project.query.statement.CreateIndexStatement;
import dbs_project.query.statement.CreateTableStatement;
import dbs_project.query.statement.DeleteRowsStatement;
import dbs_project.query.statement.DropColumnStatement;
import dbs_project.query.statement.DropIndexStatement;
import dbs_project.query.statement.DropTableStatement;
import dbs_project.query.statement.InsertRowsStatement;
import dbs_project.query.statement.QueryStatement;
import dbs_project.query.statement.RenameColumnStatement;
import dbs_project.query.statement.RenameTableStatement;
import dbs_project.query.statement.UpdateRowsStatement;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.ExtendedColumn;
import dbs_project.storage.Relation;
import dbs_project.storage.Table;
import dbs_project.storage.TableMetaData;
import dbs_project.storage.Type;
import dbs_project.storageImpl.ColumnImpl;
import dbs_project.storageImpl.RelationImpl;
import dbs_project.storageImpl.StorageLayerImpl;
import dbs_project.storageImpl.TableMetaDataImpl;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 *
 * @author kaustuv
 */
public class QueryLayerImpl implements QueryLayer {

    PersistenceLayer persistence;
    private StorageLayerImpl storage = null;

    public QueryLayerImpl() {
        storage = (StorageLayerImpl) DatabaseFactory.INSTANCE.createInstance().getStorageLayer();
        this.persistence=null;
    }

    public QueryLayerImpl(PersistenceLayer persistence) {
        this.storage = ((PersistenceLayerimpl) persistence).getStorage();
        this.persistence = persistence;
    }

    @Override
    public Relation executeQuery(QueryStatement queryStmnt) throws QueryExecutionException {
        Relation relation = null;

        try {
            relation = new RelationImpl(storage, queryStmnt.getTableNames(), queryStmnt.getColumnNames(), queryStmnt.getPredicate());
        } catch (NoSuchTableException nx) {
            throw new QueryExecutionException();
        }

        return relation;
    }

    @Override
    public int executeUpdateRows(UpdateRowsStatement updateStmnt) throws QueryExecutionException {
        try {
            TableMetaData tableMData = storage.getDatabaseSchema().get(updateStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            Table table = storage.getTable(tableMData.getId());
            RelationImpl relation = new RelationImpl(table);
            List<String> colNames = updateStmnt.getColumnNames();
            List<String> updatedValues = updateStmnt.getUpdateRowData();

            List<BitSet> resultSet = new ArrayList<>();
            relation.parseExpressionTree(resultSet, updateStmnt.getPredicate());

            if (resultSet.size() != 1) {
                throw new QueryExecutionException();
            }

            BitSet result = resultSet.get(0);

            int i = 0;
            for (; i < colNames.size(); i++) {
                ExtendedColumn col = (ExtendedColumn) table.getColumn(table.getTableMetaData().getTableSchema().get(colNames.get(i)).getId());

                int rowIndex = result.nextSetBit(0);
                while (rowIndex != -1) {
                    switch (col.getMetaData().getType()) {
                        case INTEGER:
                            col.updateValue(rowIndex + 1, Integer.parseInt(updatedValues.get(i)));
                            break;
                        case DOUBLE:
                            col.updateValue(rowIndex + 1, Double.parseDouble(updatedValues.get(i)));
                            break;
                        case STRING:
                            col.updateValue(rowIndex + 1, updatedValues.get(i));
                            break;
                        case DATE:
                            col.updateValue(rowIndex + 1, new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(updatedValues.get(i)));
                            break;
                        case BOOLEAN:
                            col.updateValue(rowIndex + 1, Boolean.parseBoolean(updatedValues.get(i)));
                            break;
                        case OBJECT:
                        //col.updateValue(rowIndex + 1, updatedValues.get(i));
                    }

                    rowIndex = result.nextSetBit(rowIndex + 1);
                }

            }

            ((StorageLayerImpl) storage).getChangedTables().put(table.getTableMetaData().getName(), (IndexableTable) table);

            if (persistence != null) {
                if (!persistence.hasActiveTransaction()) {
                    ((PersistenceLayerimpl) persistence).setActivateAuto(true);
                }

                if (!((PersistenceLayerimpl) persistence).getLastTransaction().equals("U") && !persistence.hasActiveTransaction()) {
                    ((PersistenceLayerimpl) persistence).setLastTransaction("U");
                    persistence.commitTransaction();
                }
            }
            return result.cardinality();

        } catch (NoSuchTableException | NoSuchColumnException | ParseException | InvalidKeyException | NoTransactionActiveException ex) {
            throw new QueryExecutionException(ex.getMessage());
        }
    }

    @Override
    public int executeDeleteRows(DeleteRowsStatement deleteStmnt) throws QueryExecutionException {
        try {
            TableMetaData tableMData = storage.getDatabaseSchema().get(deleteStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            Table table = storage.getTable(tableMData.getId());
            RelationImpl relation = new RelationImpl(table);
            List<BitSet> resultSet = new ArrayList<>();

            relation.parseExpressionTree(resultSet, deleteStmnt.getPredicate());

            if (resultSet.size() != 1) {
                throw new QueryExecutionException();
            }

            BitSet result = resultSet.get(0);

            int rowIndex = result.nextSetBit(0);
            while (rowIndex != -1) {
                table.deleteRow(rowIndex + 1);
                rowIndex = result.nextSetBit(rowIndex + 1);
            }

            ((StorageLayerImpl) storage).getChangedTables().put(table.getTableMetaData().getName(), (IndexableTable) table);

            if (persistence != null) {
                if (!persistence.hasActiveTransaction()) {
                    ((PersistenceLayerimpl) persistence).setActivateAuto(true);
                }

                if (!((PersistenceLayerimpl) persistence).getLastTransaction().equals("D") && !persistence.hasActiveTransaction()) {
                    ((PersistenceLayerimpl) persistence).setLastTransaction("D");
                    persistence.commitTransaction();
                }
            }
            return result.cardinality();
        } catch (NoSuchTableException | NoSuchColumnException | ParseException | NoSuchRowException | InvalidKeyException | NoTransactionActiveException ex) {
            throw new QueryExecutionException(ex.getMessage());
        }
    }

    @Override
    public void executeInsertRows(InsertRowsStatement insertStmnt) throws QueryExecutionException {
        try {
            TableMetaData tableMData = storage.getDatabaseSchema().get(insertStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            Table table = storage.getTable(tableMData.getId());
            List<String> colNames = insertStmnt.getColumnNames();
            List<ColumnImpl> colList = new ArrayList<>();
            Iterator itRow = insertStmnt.getDataForRows();

            for (String colName : colNames) {
                ColumnMetaData colMData = table.getTableMetaData().getTableSchema().get(colName);

                if (colMData == null) {
                    throw new NoSuchColumnException();
                }

                colList.add((ColumnImpl) table.getColumn(colMData.getId()));
            }

            int rowCounter = 0;

            while (itRow.hasNext()) {
                List<String> valList = (List<String>) itRow.next();

                for (int i = 0; i < colList.size(); i++) {
                    ColumnImpl column = colList.get(i);
                    String val = valList.get(i);

                    switch (column.getMetaData().getType()) {
                        case INTEGER:
                            if (val != null) {
                                column.addValue(Integer.parseInt(val));
                            } else {
                                column.addValue(Type.NULL_VALUE_INTEGER);
                            }
                            break;
                        case DOUBLE:
                            if (val != null) {
                                column.addValue(Double.parseDouble(val));
                            } else {
                                column.addValue(Type.NULL_VALUE_DOUBLE);
                            }
                            break;
                        case BOOLEAN:
                            if (val.equals("true")) {
                                column.addValue(true);
                            } else {
                                column.addValue(Type.NULL_VALUE_BOOLEAN);
                            }
                            break;
                        case DATE: {
                            if (val != null) {
                                DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                                df.setTimeZone(TimeZone.getTimeZone("UTC"));
                                column.addValue(df.parse(val));
                            }
                        }
                        break;
                        case STRING:

                        case OBJECT:
                            if (val != null) {
                                column.addValue(val);
                            }
                    }

                    if (i == 0) {
                        rowCounter++;
                    }
                }

            }

            ((TableMetaDataImpl) table.getTableMetaData()).addRowCountBy(rowCounter);
            ((StorageLayerImpl) storage).getChangedTables().put(table.getTableMetaData().getName(), (IndexableTable) table);

            if (persistence != null) {
                if (!persistence.hasActiveTransaction()) {
                    ((PersistenceLayerimpl) persistence).setActivateAuto(true);
                }

                if (!((PersistenceLayerimpl) persistence).getLastTransaction().equals("I") && !persistence.hasActiveTransaction()) {
                    ((PersistenceLayerimpl) persistence).setLastTransaction("I");
                    persistence.commitTransaction();
                }
            }
        } catch (NoSuchTableException | NoSuchColumnException | ParseException | NoTransactionActiveException ex) {
            throw new QueryExecutionException();
        }
    }

    @Override
    public void createTable(CreateTableStatement createTableStmnt) throws QueryExecutionException {
        List<String> colNames = createTableStmnt.getColumnNames();
        List<Type> colTypes = createTableStmnt.getColumnTypes();
        Map<String, Type> schema = new HashMap<>();
        String tableName = createTableStmnt.getTableName();

        if (colNames.size() != colTypes.size()) {
            throw new QueryExecutionException();
        }

        for (int i = 0; i < colNames.size(); i++) {
            schema.put(colNames.get(i), colTypes.get(i));
        }

        try {
            storage.createTable(tableName, schema);
        } catch (TableAlreadyExistsException ex) {
            throw new QueryExecutionException();
        }
    }

    @Override
    public void createColumn(CreateColumnStatement createColumnStmnt) throws QueryExecutionException {
        try {

            TableMetaData tableMData = storage.getDatabaseSchema().get(createColumnStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            Table table = storage.getTable(tableMData.getId());

            table.createColumn(createColumnStmnt.getColumnName(), createColumnStmnt.getColumnType());
        } catch (NoSuchTableException | ColumnAlreadyExistsException ex) {
            throw new QueryExecutionException();
        }
    }

    @Override
    public void createIndex(CreateIndexStatement createIndexStmnt) throws QueryExecutionException {
        try {
            TableMetaData tableMData = storage.getDatabaseSchema().get(createIndexStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            IndexableTable table = (IndexableTable) storage.getTable(tableMData.getId());

            ColumnMetaData colMetaData = tableMData.getTableSchema().get(createIndexStmnt.getColumnName());

            if (colMetaData == null) {
                throw new NoSuchColumnException();
            }

            table.createIndex(createIndexStmnt.getIndexName(), colMetaData.getId(), createIndexStmnt.getIndexType());
        } catch (NoSuchTableException | NoSuchColumnException | IndexAlreadyExistsException ex) {
            throw new QueryExecutionException();
        }

    }

    @Override
    public void dropTable(DropTableStatement dropTableStmnt) throws QueryExecutionException {
        try {
            TableMetaData tableMData = storage.getDatabaseSchema().get(dropTableStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            storage.deleteTable(storage.getDatabaseSchema().get(dropTableStmnt.getTableName()).getId());
        } catch (NoSuchTableException ex) {
            throw new QueryExecutionException();
        }
    }

    @Override
    public void dropColumn(DropColumnStatement dropColumnStmnt) throws QueryExecutionException {
        try {
            TableMetaData tableMData = storage.getDatabaseSchema().get(dropColumnStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            Table table = storage.getTable(tableMData.getId());

            ColumnMetaData colMetaData = tableMData.getTableSchema().get(dropColumnStmnt.getColumnName());

            if (colMetaData == null) {
                throw new NoSuchColumnException();
            }

            table.dropColumn(colMetaData.getId());
        } catch (NoSuchTableException | NoSuchColumnException ex) {
            throw new QueryExecutionException();
        }
    }

    @Override
    public void dropIndex(DropIndexStatement dropIndexStmnt) throws QueryExecutionException {
        try {
            TableMetaData tableMData = storage.getDatabaseSchema().get(dropIndexStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            IndexableTable table = (IndexableTable) storage.getTable(tableMData.getId());

            ColumnMetaData colMetaData = tableMData.getTableSchema().get(dropIndexStmnt.getColumnName());

            if (colMetaData == null) {
                throw new NoSuchColumnException();
            }

            List<Index> indexes = new ArrayList<>(table.getIndexes(colMetaData.getId()));

            boolean indexExists = false;
            for (Index index : indexes) {
                if (index.getIndexMetaInfo().getName().equals(dropIndexStmnt.getIndexName())) {
                    table.dropIndex(index.getIndexMetaInfo().getId());
                    indexExists = true;
                    break;
                }
            }

            if (!indexExists) {
                throw new NoSuchIndexException();
            }

        } catch (NoSuchTableException | NoSuchColumnException | NoSuchIndexException ex) {
            throw new QueryExecutionException();
        }
    }

    @Override
    public void renameTable(RenameTableStatement renameTableStmnt) throws QueryExecutionException {
        try {
            TableMetaData tableMData = storage.getDatabaseSchema().get(renameTableStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            storage.renameTable(tableMData.getId(), renameTableStmnt.getNewTableName());
        } catch (NoSuchTableException | TableAlreadyExistsException ex) {
            throw new QueryExecutionException();
        }
    }

    @Override
    public void renameColumn(RenameColumnStatement renameColumnStmnt) throws QueryExecutionException {
        try {
            TableMetaData tableMData = storage.getDatabaseSchema().get(renameColumnStmnt.getTableName());

            if (tableMData == null) {
                throw new NoSuchTableException();
            }

            Table table = storage.getTable(tableMData.getId());

            ColumnMetaData colMetaData = tableMData.getTableSchema().get(renameColumnStmnt.getColumnName());

            if (colMetaData == null) {
                throw new NoSuchColumnException();
            }

            table.renameColumn(colMetaData.getId(), renameColumnStmnt.getNewColumnName());
        } catch (NoSuchTableException | NoSuchColumnException | ColumnAlreadyExistsException ex) {
            throw new QueryExecutionException();
        }
    }

}
