package org.forome.database.schema;

import org.forome.database.exception.DatabaseException;
import org.forome.database.schema.dbstruct.DBField;
import org.forome.database.schema.dbstruct.DBSchema;
import org.forome.database.schema.dbstruct.DBTable;
import org.forome.database.schema.dbstruct.DBTableTestUtil;
import org.forome.database.utils.TypeConvert;
import org.forome.domain.ExchangeFolderReadable;
import org.forome.domain.GeneralReadable;
import org.forome.domain.StoreFileReadable;
import org.forome.domain.type.FormatType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;

class SchemaTest extends DomainDataJ5Test {

    @Test
    @DisplayName("Создание пустой схемы")
    void justCreateTest() throws DatabaseException {
        Schema schema = Schema.create(rocksDBProvider);
        DBSchema schemaDB = schema.getDbSchema();
        Assertions.assertThat(schemaDB.getTables()).isEmpty();
        Assertions.assertThat(schemaDB.getVersion()).isEqualTo(Schema.CURRENT_VERSION);
        rocksDBProvider.containsColumnFamily(Schema.SERVICE_COLUMN_FAMILY);
        String schemaJson = TypeConvert.unpackString(rocksDBProvider.getValue(Schema.SERVICE_COLUMN_FAMILY, Schema.SCHEMA_KEY));
        Assertions.assertThat(schemaJson).isEqualTo("[]");
        String versionJson = TypeConvert.unpackString(rocksDBProvider.getValue(Schema.SERVICE_COLUMN_FAMILY, Schema.VERSION_KEY));
        Assertions.assertThat(versionJson).isEqualTo(Schema.CURRENT_VERSION);
    }

    @Test
    @DisplayName("Ошибка создания уже существующей схемы")
    void schemaAlreadyExistsTest() throws DatabaseException {
        Schema.create(rocksDBProvider);
        Assertions.assertThatExceptionOfType(DatabaseException.class).isThrownBy(() -> Schema.create(rocksDBProvider));
    }

    @Test
    @DisplayName("Чтение пустой схемы")
    void readSchemaTest() throws DatabaseException {
        Schema.create(rocksDBProvider);

        Schema readSchema = Schema.read(rocksDBProvider);
        DBSchema schemaDB = readSchema.getDbSchema();
        Assertions.assertThat(schemaDB.getTables()).isEmpty();
        Assertions.assertThat(schemaDB.getVersion()).isEqualTo(Schema.CURRENT_VERSION);
    }

    @Test
    @DisplayName("Создание простой таблицы с одним полем и одним hashIndex")
    void createSimpleTable() throws DatabaseException {
        Schema schema = Schema.create(rocksDBProvider);
        StructEntity generalSE = new StructEntity(GeneralReadable.class);
        schema.createTable(generalSE);

        assertColumnFamilies("org.forome.rocksdb.general", "org.forome.rocksdb.general.index");
        DBField field = DBTableTestUtil.buildDBField(0, "value", Long.class, null);
        DBTable expected = DBTableTestUtil.buildDBTable(0, "general", "org.forome.rocksdb", new ArrayList<DBField>() {{
                    add(field);
                }});
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field));
        DBTableTestUtil.assertThatContains(rocksDBProvider, expected);
    }

    @Test
    @DisplayName("Создание таблицы с несколькими полями, одним hashIndex и зависимостью на саму себя")
    void createMultiplyFieldTable() throws DatabaseException {
        Schema schema = Schema.create(rocksDBProvider);
        StructEntity exchangeFolder = new StructEntity(ExchangeFolderReadable.class);
        schema.createTable(exchangeFolder);

        assertColumnFamilies("org.forome.exchange.ExchangeFolder", "org.forome.exchange.ExchangeFolder.index");
        DBField field1 = DBTableTestUtil.buildDBField(0, "uuid", String.class, null);
        DBField field2 = DBTableTestUtil.buildDBField(1, "email", String.class, null);
        DBField field3 = DBTableTestUtil.buildDBField(2, "date", Instant.class, null);
        DBField field4 = DBTableTestUtil.buildDBField(3, "state", String.class, null);
        DBField field5 = DBTableTestUtil.buildDBField(4, "parent_id", Long.class, 0);

        DBTable expected = DBTableTestUtil.buildDBTable(0, "ExchangeFolder", "org.forome.exchange", new ArrayList<DBField>() {{
                    add(field1);
                    add(field2);
                    add(field3);
                    add(field4);
                    add(field5);
                }});
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field5));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field2, field1));
        DBTableTestUtil.assertThatContains(rocksDBProvider, expected);
    }

    @Test
    @DisplayName("Создание таблицы с несколькими полями, всеми индексами и на другую сущность")
    void createTableWithAllIndexes() throws DatabaseException {
        Schema schema = Schema.create(rocksDBProvider);
        StructEntity general = new StructEntity(GeneralReadable.class);
        schema.createTable(general);
        StructEntity exchangeFolder = new StructEntity(ExchangeFolderReadable.class);
        schema.createTable(exchangeFolder);
        StructEntity storeFile = new StructEntity(StoreFileReadable.class);
        schema.createTable(storeFile);

        assertColumnFamilies("org.forome.store.StoreFile", "org.forome.store.StoreFile.index");
        DBField field0 = DBTableTestUtil.buildDBField(0, "name", String.class, null);
        DBField field1 = DBTableTestUtil.buildDBField(1, "type", String.class, null);
        DBField field2 = DBTableTestUtil.buildDBField(2, "size", Long.class, null);
        DBField field3 = DBTableTestUtil.buildDBField(3, "single", Boolean.class, null);
        DBField field4 = DBTableTestUtil.buildDBField(4, "format", FormatType.class, null);
        DBField field5 = DBTableTestUtil.buildDBField(5, "folder_id", Long.class, 1);
        DBField field6 = DBTableTestUtil.buildDBField(6, "double", Double.class, null);
        DBField field7 = DBTableTestUtil.buildDBField(7, "begin_time", Instant.class, null);
        DBField field8 = DBTableTestUtil.buildDBField(8, "end_time", Instant.class, null);
        DBField field9 = DBTableTestUtil.buildDBField(9, "begin", Long.class, null);
        DBField field10 = DBTableTestUtil.buildDBField(10, "end", Long.class, null);
        DBField field11 = DBTableTestUtil.buildDBField(11, "local_begin", LocalDateTime.class, null);
        DBField field12 = DBTableTestUtil.buildDBField(12, "local_end", LocalDateTime.class, null);
        DBField field13 = DBTableTestUtil.buildDBField(13, "data", byte[].class, null);

        DBTable expected = DBTableTestUtil.buildDBTable(0, "StoreFile", "org.forome.store", new ArrayList<DBField>() {{
                    add(field0);
                    add(field1);
                    add(field2);
                    add(field3);
                    add(field4);
                    add(field5);
                    add(field6);
                    add(field7);
                    add(field8);
                    add(field9);
                    add(field10);
                    add(field11);
                    add(field12);
                    add(field13);
                }});
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field5));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field2));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field0));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field0, field2));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field0, field3));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field4));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(field11));

        expected.attachIndex(DBTableTestUtil.buildDBPrefixIndex(field0));
        expected.attachIndex(DBTableTestUtil.buildDBPrefixIndex(field0, field1));

        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(field2));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(field6));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(field7));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(field11));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(field2, field0));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(field2, field5));

        expected.attachIndex(DBTableTestUtil.buildDBRangeIndex(field9, field10));
        expected.attachIndex(DBTableTestUtil.buildDBRangeIndex(field9, field10, field5));
        expected.attachIndex(DBTableTestUtil.buildDBRangeIndex(field7, field8));
        expected.attachIndex(DBTableTestUtil.buildDBRangeIndex(field7, field8, field5));
        expected.attachIndex(DBTableTestUtil.buildDBRangeIndex(field11, field12));

        DBTableTestUtil.assertThatContains(rocksDBProvider, expected);
    }

    private void assertColumnFamilies(String... cfs) {
        Assertions.assertThat(rocksDBProvider.getColumnFamilies()).contains(cfs);
    }

    // Добавить еще на чтение


    // Добавить save schema
}
