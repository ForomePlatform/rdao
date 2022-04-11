package org.forome.database.domainobject.iterator;

import org.forome.database.provider.DBIterator;
import org.forome.database.provider.KeyPattern;
import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.DataEnumerable;
import org.forome.database.schema.Schema;
import org.forome.database.schema.StructEntity;
import org.forome.database.utils.key.FieldKey;
import org.forome.database.exception.DatabaseException;

import java.lang.reflect.Constructor;
import java.util.NoSuchElementException;
import java.util.Set;

public class AllIterator<E extends DomainObject> implements IteratorEntity<E> {

    private final DataEnumerable dataEnumerable;
    private final Constructor<E> constructor;
    private final Set<Integer> loadingFields;
    private final DBIterator dataIterator;
    private final StructEntity entity;

    private final DataEnumerable.NextState state;

    public AllIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<Integer> loadingFields) throws DatabaseException {
        this.dataEnumerable = dataEnumerable;
        this.constructor = DomainObject.getConstructor(clazz);
        this.loadingFields = loadingFields;
        this.entity = Schema.getEntity(clazz);
        this.dataIterator = dataEnumerable.createIterator(entity.getColumnFamily());

        KeyPattern dataKeyPattern = loadingFields != null ? FieldKey.buildKeyPattern(entity.getFieldNames(loadingFields)) : null;
        this.state = dataEnumerable.seek(dataIterator, dataKeyPattern, entity);
        if (this.state.isEmpty()) {
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

        return dataEnumerable.nextObject(constructor, loadingFields, dataIterator, state, entity);
    }

    @Override
    public void close() throws DatabaseException {
        dataIterator.close();
    }
}
