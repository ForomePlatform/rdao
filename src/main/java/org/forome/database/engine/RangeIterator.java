package org.forome.database.engine;

import org.forome.database.domainobject.filter.RangeFilter;
import org.forome.database.domainobject.filter.SortDirection;
import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.DBDataReader;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.KeyPattern;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.dbstruct.DBBaseIntervalIndex;
import org.forome.database.schema.dbstruct.DBTable;
import org.forome.database.utils.key.RangeIndexKey;
import org.forome.database.utils.RangeIndexUtils;

import java.util.HashSet;
import java.util.Set;

public class RangeIterator extends BaseIntervalRecordIterator<RangeFilter> {

    private Set<Long> processedIds/* = null*/; // не нужно инициализировать, т.к. matchKey вызывается из конструктора базового класса

    public RangeIterator(DBTable table, RangeFilter filter, DBDataReader dataReader) {
        super(table, filter, SortDirection.ASC, dataReader);
    }

    @Override
    DBBaseIntervalIndex getIndex(RangeFilter filter, DBTable table) {
        return table.getIndex(filter);
    }

    @Override
    KeyValue seek(DBIterator indexIterator, KeyPattern pattern) throws DatabaseException {
        return RangeIndexUtils.seek(indexIterator, pattern, filterBeginValue);
    }

    @Override
    int matchKey(long id, byte[] key) {

        long indexedValue = RangeIndexKey.unpackIndexedValue(key);
        if (indexedValue > filterEndValue) {
            return KeyPattern.MATCH_RESULT_UNSUCCESS;
        } else if (indexedValue == filterEndValue) {
            if (filterBeginValue != filterEndValue) {
                return KeyPattern.MATCH_RESULT_UNSUCCESS;
            }

            return RangeIndexKey.unpackType(key) == RangeIndexKey.Type.DOT ? KeyPattern.MATCH_RESULT_SUCCESS : KeyPattern.MATCH_RESULT_CONTINUE;
        }

        if (processedIds != null && processedIds.contains(id)) {
            if (RangeIndexKey.unpackType(key) == RangeIndexKey.Type.END) {
                processedIds.remove(id);
            }
            return KeyPattern.MATCH_RESULT_CONTINUE;
        }

        if (RangeIndexKey.unpackType(key) == RangeIndexKey.Type.BEGIN) {
            if (processedIds == null) {
                processedIds = new HashSet<>();
            }
            processedIds.add(id);
        }
        return KeyPattern.MATCH_RESULT_SUCCESS;
    }
}
