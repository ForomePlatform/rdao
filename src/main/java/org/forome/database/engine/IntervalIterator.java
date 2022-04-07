package org.forome.database.engine;

import org.forome.database.domainobject.filter.IntervalFilter;
import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.DBDataReader;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.KeyPattern;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.dbstruct.DBBaseIntervalIndex;
import org.forome.database.schema.dbstruct.DBTable;
import org.forome.database.utils.key.IntervalIndexKey;

public class IntervalIterator extends BaseIntervalRecordIterator<IntervalFilter> {

    public IntervalIterator(DBTable table, IntervalFilter filter, DBDataReader dataReader) {
        super(table, filter, filter.getSortDirection(), dataReader);
    }

    @Override
    DBBaseIntervalIndex getIndex(IntervalFilter filter, DBTable table) {
        return table.getIndex(filter);
    }

    @Override
    KeyValue seek(DBIterator indexIterator, KeyPattern pattern) throws DatabaseException {
        return indexIterator.seek(pattern);
    }

    @Override
    int matchKey(long id, byte[] key) {
        long indexedBeginValue = IntervalIndexKey.unpackIndexedValue(key);
        if (indexedBeginValue < filterBeginValue || indexedBeginValue > filterEndValue) {
            return KeyPattern.MATCH_RESULT_UNSUCCESS;
        }
        return KeyPattern.MATCH_RESULT_SUCCESS;
    }
}
