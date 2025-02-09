package org.forome.database.engine;

import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.DBDataReader;
import org.forome.database.provider.DBIterator;
import org.forome.database.schema.dbstruct.DBTable;
import org.forome.database.Record;

public class AllIterator extends BaseRecordIterator {

    private final DBIterator iterator;
    private final DBTable table;
    private final NextState state;

    public AllIterator(DBTable table, DBDataReader dataReader) throws DatabaseException {
        this.iterator = dataReader.createIterator(table.getDataColumnFamily());
        this.table = table;
        state = initializeState();
    }

    @Override
    public boolean hasNext() throws DatabaseException {
        return !state.isEmpty();
    }

    @Override
    public Record next() throws DatabaseException {
        return nextRecord(table, state, iterator);
    }

    @Override
    public void close() throws DatabaseException {
        iterator.close();
    }

    private NextState initializeState() throws DatabaseException {
        return seek(null, iterator);
    }
}
