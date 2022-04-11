package org.forome.database.schema.dbstruct;

import org.forome.database.exception.DatabaseException;
import org.forome.database.schema.Schema;
import org.forome.rocksdb.RocksDBProvider;
import org.assertj.core.api.Assertions;

import java.io.Serializable;
import java.util.List;

public class DBTableTestUtil {

    public static DBTable buildDBTable(int id, String name, String namespace, List<DBField> fields) {
        return new DBTable(id, name, namespace, fields);
    }

    public static DBField buildDBField(int id, String name, Class<? extends Serializable> type, Integer foreignTableId) {
        return new DBField(id, name, type, foreignTableId);
    }

    public static DBHashIndex buildDBHashIndex(DBField... fieldIds) {
        return new DBHashIndex(fieldIds);
    }


    public static DBPrefixIndex buildDBPrefixIndex(DBField... fieldIds) {
        return new DBPrefixIndex(fieldIds);
    }

    public static DBRangeIndex buildDBRangeIndex(DBField beginFieldId, DBField endFieldId, DBField... hashFieldIds) {
        return new DBRangeIndex(beginFieldId, endFieldId, hashFieldIds);
    }

    public static DBIntervalIndex buildDBIntervalIndex(DBField indexedFieldId, DBField... hashFieldIds) {
        return new DBIntervalIndex(indexedFieldId, hashFieldIds);
    }

    public static void assertThatContains(RocksDBProvider rocksDBProvider, DBTable... expectedTables) throws DatabaseException {
        DBSchema schema = Schema.read(rocksDBProvider).getDbSchema();
        for (DBTable expected : expectedTables) {
            DBTable actual = schema.getTable(expected.getName(), expected.getNamespace());
            Assertions.assertThat(actual).isNotNull();
            Assertions.assertThat(actual.getName()).isEqualTo(expected.getName());
            Assertions.assertThat(actual.getDataColumnFamily()).isEqualTo(expected.getDataColumnFamily());
            Assertions.assertThat(actual.getIndexColumnFamily()).isEqualTo(expected.getIndexColumnFamily());
            Assertions.assertThat(actual.getSortedFields()).hasSameSizeAs(expected.getSortedFields());
            Assertions.assertThat(actual.getHashIndexes()).hasSameSizeAs(expected.getHashIndexes());
            Assertions.assertThat(actual.getIntervalIndexes()).hasSameSizeAs(expected.getIntervalIndexes());
            Assertions.assertThat(actual.getPrefixIndexes()).hasSameSizeAs(expected.getPrefixIndexes());
            Assertions.assertThat(actual.getRangeIndexes()).hasSameSizeAs(expected.getRangeIndexes());

            expected.getSortedFields().forEach(expectedField -> assertField(expectedField, actual.getField(expectedField.getName())));

            expected.getHashIndexes().forEach(expectedIndex -> assertIndex(expectedIndex, actual.getHashIndexes()));

            expected.getIntervalIndexes().forEach(expectedIndex -> assertIndex(expectedIndex, actual.getIntervalIndexes()));

            expected.getPrefixIndexes().forEach(expectedIndex -> assertIndex(expectedIndex, actual.getPrefixIndexes()));

            expected.getRangeIndexes().forEach(expectedIndex -> assertIndex(expectedIndex, actual.getRangeIndexes()));
        }
    }

    private static void assertField(DBField expected, DBField actual) {
        Assertions.assertThat(actual).isNotNull();
        Assertions.assertThat(expected.getName()).isEqualTo(actual.getName());
        Assertions.assertThat(expected.getId()).isEqualTo(actual.getId());
        Assertions.assertThat(expected.getForeignTableId()).isEqualTo(actual.getForeignTableId());
        Assertions.assertThat(expected.getType()).isEqualTo(actual.getType());
    }

    private static void assertIndex(DBHashIndex expected, List<DBHashIndex> actuals) {
        DBHashIndex actual = actuals.stream().filter(dbHashIndex -> dbHashIndex.getId() == expected.getId()).findFirst().orElse(null);
        Assertions.assertThat(actual).isNotNull();
        Assertions.assertThat(expected.getFieldIds()).containsExactly(actual.getFieldIds());
        Assertions.assertThat(expected.getId()).isEqualTo(actual.getId());
    }

    private static void assertIndex(DBPrefixIndex expected, List<DBPrefixIndex> actuals) {
        DBPrefixIndex actual = actuals.stream().filter(dbHashIndex -> dbHashIndex.getId() == expected.getId()).findFirst().orElse(null);
        Assertions.assertThat(actual).isNotNull();
        Assertions.assertThat(expected.getFieldIds()).containsExactly(actual.getFieldIds());
        Assertions.assertThat(expected.getId()).isEqualTo(actual.getId());
    }

    private static void assertIndex(DBRangeIndex expected, List<DBRangeIndex> actuals) {
        DBRangeIndex actual = actuals.stream().filter(dbHashIndex -> dbHashIndex.getId() == expected.getId()).findFirst().orElse(null);
        Assertions.assertThat(actual).isNotNull();
        Assertions.assertThat(expected.getFieldIds()).containsExactly(actual.getFieldIds());
        Assertions.assertThat(expected.getHashFieldIds()).containsExactly(actual.getHashFieldIds());
        Assertions.assertThat(expected.getId()).isEqualTo(actual.getId());
        Assertions.assertThat(expected.getBeginFieldId()).isEqualTo(actual.getBeginFieldId());
        Assertions.assertThat(expected.getEndFieldId()).isEqualTo(actual.getEndFieldId());
    }

    private static void assertIndex(DBIntervalIndex expected, List<DBIntervalIndex> actuals) {
        DBIntervalIndex actual = actuals.stream().filter(dbHashIndex -> dbHashIndex.getId() == expected.getId()).findFirst().orElse(null);
        Assertions.assertThat(actual).isNotNull();
        Assertions.assertThat(expected.getFieldIds()).containsExactly(actual.getFieldIds());
        Assertions.assertThat(expected.getHashFieldIds()).containsExactly(actual.getHashFieldIds());
        Assertions.assertThat(expected.getId()).isEqualTo(actual.getId());
        Assertions.assertThat(expected.getIndexedFieldId()).isEqualTo(actual.getIndexedFieldId());
    }
}
