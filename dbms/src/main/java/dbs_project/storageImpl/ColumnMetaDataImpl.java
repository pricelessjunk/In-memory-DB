/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbs_project.storageImpl;

import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.Table;
import dbs_project.storage.Type;
import java.io.Serializable;

/**
 *
 * @author kaustuv
 */
public class ColumnMetaDataImpl implements ColumnMetaData, Serializable {

    private static final long serialVersionUID = 7863262235394607247L;
    private int rowCount;
    private int id;
    private Table sourceTable;
    private String name;
    private Type type;
    private int rowId;

    public ColumnMetaDataImpl() {
    }

    public ColumnMetaDataImpl(Table sourceTable, String name, Type type, int id) {
        this.sourceTable = sourceTable;
        this.name = name;
        this.rowCount = 0;
        this.type = type;
        this.id = id;
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    @Override
    public Table getSourceTable() {
        return sourceTable;
    }

    @Override
    public String getLabel() {
        return sourceTable.getTableMetaData().getName() + "." + name;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public int getRowId(int positionInColumn) throws IndexOutOfBoundsException {
        return rowId;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void incrementRowCount() {
        rowCount++;
    }

    public void setRowCount(int count) {
        rowCount = count;
    }

}
