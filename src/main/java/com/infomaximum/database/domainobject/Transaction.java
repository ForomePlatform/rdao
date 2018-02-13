package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.schema.*;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.datasource.modifier.ModifierRemove;
import com.infomaximum.database.datasource.modifier.ModifierSet;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.domainobject.key.IndexKey;
import com.infomaximum.database.domainobject.key.IntervalIndexKey;
import com.infomaximum.database.exception.DataSourceDatabaseException;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.ForeignDependencyException;
import com.infomaximum.database.utils.IndexUtils;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.TypeConvert;

import java.util.*;

public class Transaction extends DataEnumerable implements AutoCloseable {

    private long transactionId = -1;
    private boolean foreignFieldEnabled = true;

    protected Transaction(DataSource dataSource) {
        super(dataSource);
    }

    public boolean isForeignFieldEnabled() {
        return foreignFieldEnabled;
    }

    public void setForeignFieldEnabled(boolean value) {
        this.foreignFieldEnabled = value;
    }

    public <T extends DomainObject & DomainObjectEditable> T create(final Class<T> clazz) throws DatabaseException {
        try {
            StructEntity entity = Schema.getEntity(clazz);

            long id = dataSource.nextId(entity.getColumnFamily());

            T domainObject = buildDomainObject(clazz, id, Collections.emptyList());

            //Принудительно указываем, что все поля отредактированы - иначе для не инициализированных полей не правильно построятся индексы
            for (EntityField field: entity.getFields()) {
                domainObject.set(field.getName(), null);
            }

            return domainObject;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    public <T extends DomainObject & DomainObjectEditable> void save(final T object) throws DatabaseException {
        Map<EntityField, Object> newValues = object.getNewValues();
        if (newValues.isEmpty()) {
            return;
        }

        ensureTransaction();

        final String columnFamily = object.getStructEntity().getColumnFamily();
        final List<Modifier> modifiers = new ArrayList<>();
        final Map<EntityField, Object> loadedValues = object.getLoadedValues();

        // update indexed values
        for (EntityIndex index : object.getStructEntity().getIndexes()){
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object.getId(), index.sortedFields, loadedValues);
                updateIndexedValue(index, object.getId(), loadedValues, newValues, modifiers);
            }
        }

        // update prefix-indexed values
        for (EntityPrefixIndex index: object.getStructEntity().getPrefixIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object.getId(), index.sortedFields, loadedValues);
                updateIndexedValue(index, object.getId(), loadedValues, newValues, modifiers);
            }
        }

        // update interval-indexed values
        for (EntityIntervalIndex index: object.getStructEntity().getIntervalIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object.getId(), index.sortedFields, loadedValues);
                updateIndexedValue(index, object.getId(), loadedValues, newValues, modifiers);
            }
        }

        // update self-object
        modifiers.add(new ModifierSet(columnFamily, new FieldKey(object.getId()).pack()));
        for (Map.Entry<EntityField, Object> newValue: newValues.entrySet()) {
            EntityField field = newValue.getKey();
            Object value = newValue.getValue();

            validateUpdatingValue(object, field, value);

            byte[] key = new FieldKey(object.getId(), field.getName()).pack();
            if (value != null) {
                byte[] bValue = TypeConvert.pack(value.getClass(), value, field.getConverter());
                modifiers.add(new ModifierSet(columnFamily, key, bValue));
            } else {
                modifiers.add(new ModifierRemove(columnFamily, key, false));
            }
        }

        modify(modifiers);

        object._flushNewValues();
    }

    public <T extends DomainObject & DomainObjectEditable> void remove(final T obj) throws DatabaseException {
        ensureTransaction();

        validateRemovingObject(obj);

        final String columnFamily = obj.getStructEntity().getColumnFamily();
        final List<Modifier> modifiers = new ArrayList<>();
        final Map<EntityField, Object> loadedValues = new HashMap<>();

        // delete indexed values
        for (EntityIndex index : obj.getStructEntity().getIndexes()) {
            tryLoadFields(columnFamily, obj.getId(), index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, modifiers);
        }

        // delete prefix-indexed values
        for (EntityPrefixIndex index: obj.getStructEntity().getPrefixIndexes()) {
            tryLoadFields(columnFamily, obj.getId(), index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, modifiers);
        }

        // delete interval-indexed values
        for (EntityIntervalIndex index: obj.getStructEntity().getIntervalIndexes()) {
            tryLoadFields(columnFamily, obj.getId(), index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, modifiers);
        }

        // delete self-object
        modifiers.add(new ModifierRemove(columnFamily, FieldKey.buildKeyPrefix(obj.getId()), true));

        modify(modifiers);
    }

    @Override
    public <T, U extends DomainObject> T getValue(final EntityField field, U obj) throws DataSourceDatabaseException {
        ensureTransaction();

        byte[] value = dataSource.getValue(obj.getStructEntity().getColumnFamily(), new FieldKey(obj.getId(), field.getName()).pack(), transactionId);
        return (T) TypeConvert.unpack(field.getType(), value, field.getConverter());
    }

    @Override
    public long createIterator(String columnFamily) throws DataSourceDatabaseException {
        ensureTransaction();

        return dataSource.createIterator(columnFamily, transactionId);
    }

    public void modify(List<Modifier> modifiers) throws DataSourceDatabaseException {
        ensureTransaction();
        dataSource.modify(modifiers, transactionId);
    }

    public void commit() throws DataSourceDatabaseException {
        if (transactionId != -1) {
            dataSource.commitTransaction(transactionId);
            transactionId = -1;
        }
    }

    @Override
    public void close() throws DataSourceDatabaseException {
        if (transactionId != -1) {
            dataSource.rollbackTransaction(transactionId);
        }
    }

    private void ensureTransaction() throws DataSourceDatabaseException {
        if (transactionId == -1) {
            transactionId = dataSource.beginTransaction();
        }
    }

    private void tryLoadFields(String columnFamily, long id, final List<EntityField> fields, Map<EntityField, Object> loadedValues) throws DataSourceDatabaseException {
        for (EntityField field: fields) {
            tryLoadField(columnFamily, id, field, loadedValues);
        }
    }

    private void tryLoadField(String columnFamily, long id, EntityField field, Map<EntityField, Object> loadedValues) throws DataSourceDatabaseException {
        if (loadedValues.containsKey(field)) {
            return;
        }

        final byte[] key = new FieldKey(id, field.getName()).pack();
        final byte[] value = dataSource.getValue(columnFamily, key, transactionId);
        loadedValues.put(field, TypeConvert.unpack(field.getType(), value, field.getConverter()));
    }

    static void updateIndexedValue(EntityIndex index, long id, Map<EntityField, Object> prevValues, Map<EntityField, Object> newValues, List<Modifier> destination) {
        final IndexKey indexKey = new IndexKey(id, new long[index.sortedFields.size()]);

        // Remove old value-index
        IndexUtils.setHashValues(index.sortedFields, prevValues, indexKey.getFieldValues());
        destination.add(new ModifierRemove(index.columnFamily, indexKey.pack(), false));

        // Add new value-index
        for (int i = 0; i < index.sortedFields.size(); ++i) {
            EntityField field = index.sortedFields.get(i);
            Object value = newValues.containsKey(field) ? newValues.get(field) : prevValues.get(field);
            indexKey.getFieldValues()[i] = IndexUtils.buildHash(field.getType(), value, field.getConverter());
        }
        destination.add(new ModifierSet(index.columnFamily, indexKey.pack()));
    }

    static void removeIndexedValue(EntityIndex index, long id, Map<EntityField, Object> values, List<Modifier> destination) {
        final IndexKey indexKey = new IndexKey(id, new long[index.sortedFields.size()]);

        IndexUtils.setHashValues(index.sortedFields, values, indexKey.getFieldValues());
        destination.add(new ModifierRemove(index.columnFamily, indexKey.pack(), false));
    }

    private void updateIndexedValue(EntityPrefixIndex index, long id, Map<EntityField, Object> prevValues, Map<EntityField, Object> newValues, List<Modifier> destination) throws DataSourceDatabaseException {
        List<String> deletingLexemes = new ArrayList<>();
        List<String> insertingLexemes = new ArrayList<>();
        PrefixIndexUtils.diffIndexedLexemes(index.sortedFields, prevValues, newValues, deletingLexemes, insertingLexemes);

        PrefixIndexUtils.removeIndexedLexemes(index, id, deletingLexemes, destination, dataSource, transactionId);
        PrefixIndexUtils.insertIndexedLexemes(index, id, insertingLexemes, destination, dataSource, transactionId);
    }

    private void removeIndexedValue(EntityPrefixIndex index, long id, Map<EntityField, Object> values, List<Modifier> destination) throws DataSourceDatabaseException {
        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        for (EntityField field : index.sortedFields) {
            PrefixIndexUtils.splitIndexingTextIntoLexemes((String) values.get(field), lexemes);
        }

        PrefixIndexUtils.removeIndexedLexemes(index, id, lexemes, destination, dataSource, transactionId);
    }

    static void updateIndexedValue(EntityIntervalIndex index, long id, Map<EntityField, Object> prevValues, Map<EntityField, Object> newValues, List<Modifier> destination) {
        final List<EntityField> hashedFields = index.getHashedFields();
        final EntityField indexedField = index.getIndexedField();
        final IntervalIndexKey indexKey = new IntervalIndexKey(id, new long[hashedFields.size()]);

        // Remove old value-index
        IndexUtils.setHashValues(hashedFields, prevValues, indexKey.getHashedValues());
        indexKey.setIndexedValue(prevValues.get(indexedField));
        destination.add(new ModifierRemove(index.columnFamily, indexKey.pack(), false));

        // Add new value-index
        for (int i = 0; i < hashedFields.size(); ++i) {
            EntityField field = hashedFields.get(i);
            Object value = newValues.containsKey(field) ? newValues.get(field) : prevValues.get(field);
            indexKey.getHashedValues()[i] = IndexUtils.buildHash(field.getType(), value, field.getConverter());
        }
        indexKey.setIndexedValue(newValues.containsKey(indexedField) ? newValues.get(indexedField) : prevValues.get(indexedField));
        destination.add(new ModifierSet(index.columnFamily, indexKey.pack()));
    }

    static void removeIndexedValue(EntityIntervalIndex index, long id, Map<EntityField, Object> values, List<Modifier> destination) {
        final List<EntityField> hashedFields = index.getHashedFields();
        final IntervalIndexKey indexKey = new IntervalIndexKey(id, new long[hashedFields.size()]);

        IndexUtils.setHashValues(hashedFields, values, indexKey.getHashedValues());
        indexKey.setIndexedValue(values.get(index.getIndexedField()));

        destination.add(new ModifierRemove(index.columnFamily, indexKey.pack(), false));
    }

    private static boolean anyChanged(List<EntityField> fields, Map<EntityField, Object> newValues) {
        for (EntityField iField: fields) {
            if (newValues.containsKey(iField)) {
                return true;
            }
        }
        return false;
    }

    private void validateUpdatingValue(DomainObject obj, EntityField field, Object value) throws DatabaseException {
        if (value == null) {
            return;
        }

        if (!foreignFieldEnabled || !field.isForeign()) {
            return;
        }

        long fkeyIdValue = (Long) value;
        if (dataSource.getValue(field.getForeignDependency().getColumnFamily(), new FieldKey(fkeyIdValue).pack(), transactionId) == null) {
            throw new ForeignDependencyException(obj.getId(), obj.getStructEntity().getObjectClass(), field, fkeyIdValue);
        }
    }

    private void validateRemovingObject(DomainObject obj) throws DatabaseException {
        if (!foreignFieldEnabled) {
            return;
        }

        List<StructEntity.Reference> references = obj.getStructEntity().getReferencingForeignFields();
        if (references.isEmpty()) {
            return;
        }

        KeyPattern keyPattern = IndexKey.buildKeyPattern(obj.getId());
        for (StructEntity.Reference ref : references) {
            long iteratorId = dataSource.createIterator(ref.fieldIndex.columnFamily, transactionId);
            try {
                KeyValue keyValue = dataSource.seek(iteratorId, keyPattern);
                if (keyValue != null) {
                    long referencingId = IndexKey.unpackId(keyValue.getKey());
                    throw new ForeignDependencyException(obj.getId(), obj.getStructEntity().getObjectClass(), referencingId, ref.objClass);
                }
            } finally {
                dataSource.closeIterator(iteratorId);
            }
        }
    }
}
