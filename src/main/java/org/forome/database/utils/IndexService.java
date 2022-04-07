package org.forome.database.utils;

import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.DomainObjectSource;
import org.forome.database.domainobject.filter.EmptyFilter;
import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.DBProvider;
import org.forome.database.provider.DBTransaction;
import org.forome.database.schema.*;
import org.forome.database.schema.dbstruct.*;
import org.forome.database.utils.key.IntervalIndexKey;
import org.forome.database.utils.key.RangeIndexKey;
import org.infomaximum.database.schema.*;
import org.infomaximum.database.schema.dbstruct.*;
import org.forome.database.utils.key.HashIndexKey;

import java.util.*;
import java.util.stream.Collectors;

public class IndexService {

    @FunctionalInterface
    private interface ModifierCreator {
        void apply(final DomainObject obj, DBTransaction transaction) throws DatabaseException;
    }

    //todo check existed indexes and throw exception
    public static void doIndex(HashIndex index, StructEntity table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final HashIndexKey indexKey = new HashIndexKey(0, index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(index.sortedFields, obj, indexKey.getFieldValues());

            transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    public static void doIndex(DBHashIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = Arrays.stream(index.getFieldIds()).boxed().collect(Collectors.toSet());
        final HashIndexKey indexKey = new HashIndexKey(0, index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(IndexUtils.getFieldsByIds(table.getSortedFields(), index.getFieldIds()),
                    obj,
                    indexKey.getFieldValues());

            transaction.put(table.getIndexColumnFamily(), indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    public static void doPrefixIndex(PrefixIndex index, StructEntity table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            lexemes.clear();
            for (Field field : index.sortedFields) {
                PrefixIndexUtils.splitIndexingTextIntoLexemes(obj.get(field.getNumber()), lexemes);
            }
            PrefixIndexUtils.insertIndexedLexemes(index, obj.getId(), lexemes, transaction);
        });
    }

    public static void doPrefixIndex(DBPrefixIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = Arrays.stream(index.getFieldIds()).boxed().collect(Collectors.toSet());
        final SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            lexemes.clear();
            for (DBField field : IndexUtils.getFieldsByIds(table.getSortedFields(), index.getFieldIds())) {
                PrefixIndexUtils.splitIndexingTextIntoLexemes(obj.get(field.getId()), lexemes);
            }
            PrefixIndexUtils.insertIndexedLexemes(index, obj.getId(), lexemes, table.getIndexColumnFamily(), transaction);
        });
    }

    public static void doIntervalIndex(IntervalIndex index, StructEntity table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final List<Field> hashedFields = index.getHashedFields();
        final Field indexedField = index.getIndexedField();
        final IntervalIndexKey indexKey = new IntervalIndexKey(0, new long[hashedFields.size()], index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            indexKey.setIndexedValue(obj.get(indexedField.getNumber()));

            transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    public static void doIntervalIndex(DBIntervalIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = Arrays.stream(index.getFieldIds()).boxed().collect(Collectors.toSet());
        final DBField[] hashedFields = IndexUtils.getFieldsByIds(table.getSortedFields(), index.getHashFieldIds());
        final DBField indexedField = IndexUtils.getFieldsByIds(table.getSortedFields(), index.getIndexedFieldId());
        final IntervalIndexKey indexKey = new IntervalIndexKey(0, new long[hashedFields.length], index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            indexKey.setIndexedValue(obj.get(indexedField.getId()));

            transaction.put(table.getIndexColumnFamily(), indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    public static void doRangeIndex(RangeIndex index, StructEntity table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final List<Field> hashedFields = index.getHashedFields();
        final RangeIndexKey indexKey = new RangeIndexKey(0, new long[hashedFields.size()], index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            RangeIndexUtils.insertIndexedRange(index, indexKey,
                    obj.get(index.getBeginIndexedField().getNumber()),
                    obj.get(index.getEndIndexedField().getNumber()),
                    transaction);
        });
    }

    public static void doRangeIndex(DBRangeIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = Arrays.stream(index.getFieldIds()).boxed().collect(Collectors.toSet());
        final DBField[] hashedFields = IndexUtils.getFieldsByIds(table.getSortedFields(), index.getHashFieldIds());
        final RangeIndexKey indexKey = new RangeIndexKey(0, new long[hashedFields.length], index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            RangeIndexUtils.insertIndexedRange(index,
                    indexKey,
                    obj.get(index.getBeginFieldId()),
                    obj.get(index.getEndFieldId()),
                    table.getIndexColumnFamily(),
                    transaction);
        });
    }


    private static void indexData(Set<Integer> loadingFields, StructEntity table, DBProvider dbProvider, ModifierCreator recordCreator) throws DatabaseException {
        DomainObjectSource domainObjectSource = new DomainObjectSource(dbProvider);
        try (DBTransaction transaction = dbProvider.beginTransaction();
             IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(table.getObjectClass(), EmptyFilter.INSTANCE, loadingFields)) {
             while (iter.hasNext()) {
                 recordCreator.apply(iter.next(), transaction);
             }
            transaction.commit();
        }
    }

    private static void indexData(Set<Integer> loadingFields, DBTable table, DBProvider dbProvider, ModifierCreator recordCreator) throws DatabaseException {
        DomainObjectSource domainObjectSource = new DomainObjectSource(dbProvider);
        try (DBTransaction transaction = dbProvider.beginTransaction();
             IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(Schema.getTableClass(table.getName(), table.getNamespace()),
                     EmptyFilter.INSTANCE,
                     loadingFields)) {
             while (iter.hasNext()) {
                 recordCreator.apply(iter.next(), transaction);
             }
            transaction.commit();
        }
    }
}
