package dbs_project.utilImpl;

import dbs_project.util.IdCursor;
import java.io.IOException;
import org.apache.commons.collections.primitives.ArrayIntList;

/**
 *
 * @author kaustuv
 */
public class IdCursorImpl implements IdCursor {

    private ArrayIntList data;
    private int cursor;

    public IdCursorImpl() {
        cursor = -1;
    }

    public IdCursorImpl(ArrayIntList data) {
        this.data = data;
        cursor = -1;
    }

    @Override
    public boolean next() {
        if (data == null) {
            return false;
        }

        ++cursor;
        return cursor < data.size();
    }

    @Override
    public void close() throws IOException {
        data.clear();
    }

    @Override
    public int getId() {
        return data.get(cursor);
    }

}
