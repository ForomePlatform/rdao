package org.forome.database.domainobject.iterator;

import org.forome.database.domainobject.DataEnumerable;
import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.filter.BaseIntervalFilter;
import org.forome.database.domainobject.filter.SortDirection;
import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.KeyPattern;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.BaseIntervalIndex;
import org.forome.database.schema.Field;
import org.forome.database.schema.StructEntity;
import org.forome.database.utils.HashIndexUtils;
import org.forome.database.utils.IntervalIndexUtils;
import org.forome.database.utils.key.BaseIntervalIndexKey;

import java.util.*;

abstract class BaseIntervalIndexIterator<E extends DomainObject, F extends BaseIntervalFilter> extends BaseIndexIterator<E> {

    private final List<Field> checkedFilterFields;
    private final List<Object> filterValues;
    private final DBIterator.StepDirection direction;
    private final KeyPattern indexPattern;

    private KeyValue indexKeyValue;

    final long filterBeginValue, filterEndValue;

    BaseIntervalIndexIterator(DataEnumerable dataEnumerable,
                              Class<E> clazz,
                              Set<Integer> loadingFields,
                              SortDirection direction,
                              F filter) throws DatabaseException {
        super(dataEnumerable, clazz, loadingFields);
        this.direction = direction == SortDirection.ASC ? DBIterator.StepDirection.FORWARD : DBIterator.StepDirection.BACKWARD;

        Map<Integer, Object> filters = filter.getHashedValues();
        BaseIntervalIndex index = getIndex(filter, entity);

        List<Field> filterFields = null;
        List<Object> filterValues = null;

        final List<Field> hashedFields = index.getHashedFields();
        long[] values = new long[hashedFields.size()];
        for (int i = 0; i < hashedFields.size(); ++i) {
            Field field = hashedFields.get(i);
            Object value = filters.get(field.getNumber());
            if (value != null) {
                field.throwIfNotMatch(value.getClass());
            }

            values[i] = HashIndexUtils.buildHash(field.getType(), value, field.getConverter());
            if (HashIndexUtils.toLongCastable(field.getType())) {
                continue;
            }

            if (filterFields == null) {
                filterFields = new ArrayList<>();
                filterValues = new ArrayList<>();
            }

            filterFields.add(field);
            filterValues.add(value);
        }

        index.checkIndexedValueType(filter.getBeginValue().getClass());
        index.checkIndexedValueType(filter.getEndValue().getClass());

        this.checkedFilterFields = filterFields != null ? filterFields : Collections.emptyList();
        this.filterValues = filterValues;

        this.dataKeyPattern = buildDataKeyPattern(filterFields, loadingFields, entity);
        if (this.dataKeyPattern != null) {
            this.dataIterator = dataEnumerable.createIterator(entity.getColumnFamily());
        }

        this.filterBeginValue = IntervalIndexUtils.castToLong(filter.getBeginValue());
        this.filterEndValue = IntervalIndexUtils.castToLong(filter.getEndValue());
        IntervalIndexUtils.checkInterval(filterBeginValue, filterEndValue);
        this.indexIterator = dataEnumerable.createIterator(index.columnFamily);

        switch (this.direction) {
            case FORWARD:
                this.indexPattern = BaseIntervalIndexKey.buildLeftBorder(values, filterBeginValue, index);
                break;
            case BACKWARD:
                this.indexPattern = BaseIntervalIndexKey.buildRightBorder(values, filterEndValue, index);
                break;
            default:
                throw new IllegalArgumentException("direction = " + direction);
        }
        this.indexKeyValue = seek(indexIterator, indexPattern);

        nextImpl();
    }

    abstract BaseIntervalIndex getIndex(F filter, StructEntity entity);
    abstract KeyValue seek(DBIterator indexIterator, KeyPattern pattern) throws DatabaseException;

    @Override
    void nextImpl() throws DatabaseException {
        while (indexKeyValue != null) {
            final long id = BaseIntervalIndexKey.unpackId(indexKeyValue.getKey());
            final int res = matchKey(id, indexKeyValue.getKey());
            if (res == KeyPattern.MATCH_RESULT_SUCCESS) {
                nextElement = findObject(id);
            } else if (res == KeyPattern.MATCH_RESULT_CONTINUE) {
                nextElement = null;
            } else {
                break;
            }
            indexKeyValue = indexIterator.step(direction);
            if (indexKeyValue != null && indexPattern.match(indexKeyValue.getKey()) != KeyPattern.MATCH_RESULT_SUCCESS) {
                indexKeyValue = null;
            }
            if (nextElement != null) {
                return;
            }
        }

        nextElement = null;
        close();
    }

    /**
     * @return KeyPattern.MATCH_RESULT_*
     */
    abstract int matchKey(long id, byte[] key);

    @Override
    boolean checkFilter(E obj) throws DatabaseException {
        for (int i = 0; i < checkedFilterFields.size(); ++i) {
            Field field = checkedFilterFields.get(i);
            if (!HashIndexUtils.equals(field.getType(), filterValues.get(i), obj.get(field.getNumber()))) {
                return false;
            }
        }

        return true;
    }
}

