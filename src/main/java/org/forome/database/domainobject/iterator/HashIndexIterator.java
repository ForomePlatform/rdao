package org.forome.database.domainobject.iterator;

import org.forome.database.domainobject.DataEnumerable;
import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.Field;
import org.forome.database.schema.HashIndex;
import org.forome.database.utils.HashIndexUtils;
import org.forome.database.utils.key.HashIndexKey;

import java.util.*;

public class HashIndexIterator<E extends DomainObject> extends BaseIndexIterator<E> {

    private final List<Field> checkedFilterFields;
    private final List<Object> filterValues;

    private KeyValue indexKeyValue;

    public HashIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<Integer> loadingFields, HashFilter filter) throws DatabaseException {
        super(dataEnumerable, clazz, loadingFields);

        Map<Integer, Object> filters = filter.getValues();
        final HashIndex index = entity.getHashIndex(filters.keySet());

        List<Field> filterFields = null;
        List<Object> filterValues = null;

        long[] values = new long[index.sortedFields.size()];
        for (int i = 0; i < index.sortedFields.size(); ++i) {
            Field field = index.sortedFields.get(i);
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

        this.checkedFilterFields = filterFields != null ? filterFields : Collections.emptyList();
        this.filterValues = filterValues;

        this.dataKeyPattern = buildDataKeyPattern(filterFields, loadingFields, entity);
        if (this.dataKeyPattern != null) {
            this.dataIterator = dataEnumerable.createIterator(entity.getColumnFamily());
        }

        this.indexIterator = dataEnumerable.createIterator(index.columnFamily);
        this.indexKeyValue = indexIterator.seek(HashIndexKey.buildKeyPattern(index, values));

        nextImpl();
    }

    @Override
    void nextImpl() throws DatabaseException {
        while (indexKeyValue != null) {
            nextElement = findObject(HashIndexKey.unpackId(indexKeyValue.getKey()));
            indexKeyValue = indexIterator.next();
            if (nextElement != null) {
                return;
            }
        }

        nextElement = null;
        close();
    }

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
