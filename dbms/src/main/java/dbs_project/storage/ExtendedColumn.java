package dbs_project.storage;

import dbs_project.index.Index;
import java.text.ParseException;
import java.util.List;

/**
 *
 * @author kaustuv
 */
public interface ExtendedColumn extends Column {

    public void addValue(Object o) throws ParseException;

    public void updateValue(int rowId, Object o) throws ParseException;

    public void removeValue(int rowId) throws ParseException;

    public Object getData();

    public TableMetaData getSrcTabMet();

    public void setSrcTabMet(TableMetaData srcTabMet);

    public List<Index> getIndexes();

}
