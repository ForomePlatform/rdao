package org.forome.database.domainobject.iterator;

import org.forome.database.domainobject.DataEnumerable;
import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.filter.IntervalFilter;
import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.KeyPattern;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.BaseIntervalIndex;
import org.forome.database.schema.StructEntity;
import org.forome.database.utils.key.IntervalIndexKey;

import java.util.*;

public class IntervalIndexIterator<E extends DomainObject> extends BaseIntervalIndexIterator<E, IntervalFilter> {

    public IntervalIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<Integer> loadingFields, IntervalFilter filter) throws DatabaseException {
        super(dataEnumerable, clazz, loadingFields, filter.getSortDirection(), filter);
    }

    @Override
    BaseIntervalIndex getIndex(IntervalFilter filter, StructEntity entity) {
        return entity.getIntervalIndex(filter.getHashedValues().keySet(), filter.getIndexedFieldId());
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
