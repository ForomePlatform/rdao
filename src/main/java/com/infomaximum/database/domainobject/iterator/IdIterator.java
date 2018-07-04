package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.IdFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.utils.key.FieldKey;

import java.lang.reflect.Constructor;
import java.util.NoSuchElementException;
import java.util.Set;

public class IdIterator<E extends DomainObject> implements IteratorEntity<E> {

    private final DataEnumerable dataEnumerable;
    private final Constructor<E> constructor;
    private final Set<String> loadingFields;
    private final DBIterator dataIterator;

    private final DataEnumerable.NextState state;
    private final long endId;

    public IdIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, IdFilter filter) throws DatabaseException {
        this.dataEnumerable = dataEnumerable;
        this.constructor = DomainObject.getConstructor(clazz);
        this.loadingFields = loadingFields;
        this.endId = filter.getToId();
        String columnFamily = Schema.getEntity(clazz).getColumnFamily();
        this.dataIterator = dataEnumerable.createIterator(columnFamily);

        KeyPattern dataKeyPattern;
        if (loadingFields != null) {
            dataKeyPattern = new KeyPattern(FieldKey.buildKeyPrefix(filter.getFromId()), 0, FieldKey.buildInnerPatterns(loadingFields));
        } else {
            dataKeyPattern = new KeyPattern(FieldKey.buildKeyPrefix(filter.getFromId()), 0);
        }
        this.state = dataEnumerable.seek(dataIterator, dataKeyPattern);
        if (endReached()) {
            state.reset();
            close();
        }
    }

    @Override
    public boolean hasNext() {
        return !state.isEmpty();
    }

    @Override
    public E next() throws DatabaseException {
        if (state.isEmpty()) {
            throw new NoSuchElementException();
        }

        E result = dataEnumerable.nextObject(constructor, loadingFields, dataIterator, state);
        if (endReached()) {
            state.reset();
            close();
        }

        return result;
    }

    @Override
    public void close() throws DatabaseException {
        dataIterator.close();
    }

    private boolean endReached() {
        return state.isEmpty() || state.getNextId() > endId;
    }
}
