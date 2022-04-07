package org.forome.database.schema;

import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.filter.*;
import org.forome.database.exception.*;
import org.forome.database.schema.table.*;
import org.forome.domain.*;
import org.infomaximum.database.domainobject.filter.*;
import org.forome.database.domainobject.iterator.IteratorEntity;
import org.infomaximum.database.exception.*;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.dbstruct.DBTable;
import org.infomaximum.database.schema.table.*;
import org.infomaximum.domain.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class SchemaTableTest extends DomainDataJ5Test {

    @BeforeEach
    void setUp() throws DatabaseException {
        schema = Schema.create(rocksDBProvider);
    }

    private Schema schema;

    //Создание таблицы_____________________________________________
    @Test
    @DisplayName("Создает простую таблицу с полями без зависимостей")
    void createTableOnlyFieldsTest() throws DatabaseException {
        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("field1", String.class));
            add(new TField("field2", Integer.class));
            add(new TField("field3", Long.class));
        }};
        Table table = new Table("dataTest", "org.infomaximum.exchange", fields);
        schema.createTable(table);

        assertThatSchemaContainsTable(table);
    }

    @Test
    @DisplayName("Создает простую таблицу с полями без зависимостей и хеш индексом")
    void createTableFieldsAndIndexesTest() throws DatabaseException {
        Schema.resolve(GeneralReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("value", String.class));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("value"));
        }};
        Table table = new Table("general", "org.infomaximum.rocksdb", fields, hashIndexes);
        schema.createTable(table);

        assertThatSchemaContainsTable(table);
    }

    @Test
    @DisplayName("Создает таблицу с зависимостью на саму себя")
    void createTableWithSelfDependenciesTest() throws DatabaseException {
        Schema.resolve(ExchangeFolderReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("uuid", String.class));
            add(new TField("email", String.class));
            add(new TField("date", Instant.class));
            add(new TField("state", String.class));
            add(new TField("parent_id", new TableReference("ExchangeFolder", "org.infomaximum.exchange")));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("email", "uuid"));
        }};
        Table table = new Table("ExchangeFolder", "org.infomaximum.exchange", fields, hashIndexes);
        schema.createTable(table);


        List<THashIndex> expectedHashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("parent_id"));
            add(new THashIndex("email", "uuid"));
        }};
        Table expected = new Table("ExchangeFolder", "org.infomaximum.exchange", fields, expectedHashIndexes);
        assertThatSchemaContainsTable(expected);
    }

    @Test
    @DisplayName("Создает таблицу с полями с зависимостями и всеми индексами")
    void createTableWithDependenciesAndIndexesTest() throws Exception {
        createExchangeFolderTable();
        Schema.resolve(StoreFileReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("name", String.class));
            add(new TField("type", String.class));
            add(new TField("size", Long.class));
            add(new TField("single", Boolean.class));
            add(new TField("format", String.class));
            add(new TField("folder_id", new TableReference("ExchangeFolder", "org.infomaximum.exchange")));
            add(new TField("double", Double.class));
            add(new TField("begin_time", Instant.class));
            add(new TField("end_time", Instant.class));
            add(new TField("begin", Long.class));
            add(new TField("end", Long.class));
            add(new TField("local_begin", LocalDateTime.class));
            add(new TField("local_end", LocalDateTime.class));
            add(new TField("data", byte[].class));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("size"));
            add(new THashIndex("name"));
            add(new THashIndex("size", "name"));
            add(new THashIndex("format"));
            add(new THashIndex("local_begin"));
        }};
        List<TPrefixIndex> prefixIndexes = new ArrayList<TPrefixIndex>() {{
            add(new TPrefixIndex("name"));
            add(new TPrefixIndex("name", "type"));
        }};
        List<TIntervalIndex> intervalIndexes = new ArrayList<TIntervalIndex>() {{
            add(new TIntervalIndex("size"));
            add(new TIntervalIndex("double"));
            add(new TIntervalIndex("begin_time"));
            add(new TIntervalIndex("local_begin"));
            add(new TIntervalIndex("size", new String[]{"name"}));
            add(new TIntervalIndex("size", new String[]{"folder_id"}));
        }};
        List<TRangeIndex> rangeIndexes = new ArrayList<TRangeIndex>() {{
            add(new TRangeIndex("begin", "end"));
            add(new TRangeIndex("begin", "end", new String[]{"folder_id"}));
            add(new TRangeIndex("begin_time", "end_time"));
            add(new TRangeIndex("begin_time", "begin_time", new String[]{"folder_id"}));
            add(new TRangeIndex("local_begin", "local_end"));
        }};
        Table table = new Table("StoreFile",
                "org.infomaximum.store",
                fields,
                hashIndexes,
                prefixIndexes,
                intervalIndexes,
                rangeIndexes
        );
        schema.createTable(table);

        hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("folder_id"));
            add(new THashIndex("size"));
            add(new THashIndex("name"));
            add(new THashIndex("name", "size"));
            add(new THashIndex("format"));
            add(new THashIndex("local_begin"));
        }};
        Table expected = new Table("StoreFile",
                "org.infomaximum.store",
                fields,
                hashIndexes,
                prefixIndexes,
                intervalIndexes,
                rangeIndexes
        );
        assertThatSchemaContainsTable(expected);
    }

    @Test
    @DisplayName("Очищает таблицу с полями с зависимостями и всеми индексами")
    void clearTableWithDependenciesAndIndexesTest() throws Exception {
        Table exchangeFolderTable = createExchangeFolderTable();
        Table storeFolderTable = createStoreFolderTable();
        //Добавляем новые данные в таблицу
        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable ex = transaction.create(ExchangeFolderEditable.class);
            ex.setUuid("1243");
            ex.setUserEmail("email");
            transaction.save(ex);

            ExchangeFolderEditable ex2 = transaction.create(ExchangeFolderEditable.class);
            ex.setUuid("12435");
            ex.setUserEmail("email2");
            ex.setParentId(ex.getId());
            transaction.save(ex2);

            StoreFileEditable storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(123L);
            storeFileEditable.setBegin(124L);
            storeFileEditable.setEnd(144L);
            storeFileEditable.setFileName("FileName2");
            storeFileEditable.setFolderId(ex.getId());
            storeFileEditable.setDouble(12.7);
            transaction.save(storeFileEditable);

            storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(11L);
            storeFileEditable.setBegin(1L);
            storeFileEditable.setEnd(2L);
            storeFileEditable.setFolderId(ex2.getId());
            storeFileEditable.setFileName("FileName2");
            storeFileEditable.setDouble(13.4);
            transaction.save(storeFileEditable);
        });


        //Делаем очистку
        schema.clearTable("StoreFile", "org.infomaximum.store");
        assertThatTableIsEmpty(storeFolderTable);

        schema.clearTable("ExchangeFolder", "org.infomaximum.exchange");
        assertThatTableIsEmpty(exchangeFolderTable);

        Table expectedStoreFile = new Table("StoreFile",
                "org.infomaximum.store",
                storeFolderTable.getFields(),
                storeFolderTable.getHashIndexes(),
                storeFolderTable.getPrefixIndexes(),
                storeFolderTable.getIntervalIndexes(),
                storeFolderTable.getRangeIndexes()
        );
        assertThatSchemaContainsTable(expectedStoreFile);

        Table expected = new Table("ExchangeFolder",
                "org.infomaximum.exchange",
                exchangeFolderTable.getFields(),
                exchangeFolderTable.getHashIndexes(),
                exchangeFolderTable.getPrefixIndexes(),
                exchangeFolderTable.getIntervalIndexes(),
                exchangeFolderTable.getRangeIndexes()
        );
        assertThatSchemaContainsTable(expected);
    }

    @Test
    @DisplayName("Проверка ошибки очистки таблицы из-за наличия зависимости от этой таблицы")
    void errorClearTableWithDependenciesTest() throws Exception {
        Table exchangeFolderTable = createExchangeFolderTable();
        Table storeFolderTable = createStoreFolderTable();
        //Добавляем новые данные в таблицу
        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable ex = transaction.create(ExchangeFolderEditable.class);
            ex.setUuid("1243");
            ex.setUserEmail("email");
            transaction.save(ex);

            ExchangeFolderEditable ex2 = transaction.create(ExchangeFolderEditable.class);
            ex.setUuid("12435");
            ex.setUserEmail("email2");
            ex.setParentId(ex.getId());
            transaction.save(ex2);

            StoreFileEditable storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(123L);
            storeFileEditable.setBegin(124L);
            storeFileEditable.setEnd(144L);
            storeFileEditable.setFileName("FileName2");
            storeFileEditable.setFolderId(ex.getId());
            storeFileEditable.setDouble(12.7);
            transaction.save(storeFileEditable);

            storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(11L);
            storeFileEditable.setBegin(1L);
            storeFileEditable.setEnd(2L);
            storeFileEditable.setFolderId(ex2.getId());
            storeFileEditable.setFileName("FileName2");
            storeFileEditable.setDouble(13.4);
            transaction.save(storeFileEditable);
        });

        Assertions.assertThatThrownBy(() -> schema.clearTable("ExchangeFolder", "org.infomaximum.exchange")).isInstanceOf(TableClearException.class);
    }

    //Удаление таблицы_____________________________________________
    @Test
    @DisplayName("Удаляет простую таблицу")
    void removeTableTest() throws Exception {
        createExchangeFolderTable();
        createStoreFolderTable();

        schema.dropTable("StoreFile", "org.infomaximum.store");
        assertTableDoesntExist("StoreFile", "org.infomaximum.store");

        schema.dropTable("ExchangeFolder", "org.infomaximum.exchange");
        assertTableDoesntExist("ExchangeFolder", "org.infomaximum.exchange");
    }

    @Test
    @DisplayName("Ошибка при удалени таблицы, на которую ссылается другая таблица")
    void failBecauseRemoveDependencedTableTest() throws Exception {
        createExchangeFolderTable();
        createStoreFolderTable();

        Assertions.assertThatThrownBy(() -> schema.dropTable("ExchangeFolder", "org.infomaximum.exchange"))
                .isExactlyInstanceOf(TableRemoveException.class);
    }

    //Добавление полей_____________________________________________
    @Test
    @DisplayName("Добавляет поле в таблицу")
    void createTableFieldTest() throws Exception {
        Table generalTable = createGeneralTable();
        TField newField = new TField("newField", Long.class);
        schema.createField(newField, generalTable);

        List<TField> newFields = new ArrayList<>(generalTable.getFields());
        newFields.add(newField);
        generalTable = new Table(generalTable.getName(), generalTable.getNamespace(), newFields, generalTable.getHashIndexes());
        assertThatSchemaContainsTable(generalTable);
    }

    @Test
    @DisplayName("Ошибка добавления поля в таблицу, которое зависит от не существующей таблицы")
    void failCreateFieldBecauseForeignTableDoesntExist() throws Exception {
        createGeneralTable();
        TField newFieldWithDependence = new TField("newFieldWithDependence", new TableReference("ExchangeFolder",
                "org.infomaximum.exchange"));
        Assertions.assertThatThrownBy(() -> schema.createField(newFieldWithDependence, "general", "org.infomaximum.rocksdb"))
                .isExactlyInstanceOf(TableNotFoundException.class);
    }

    @Test
    @DisplayName("Ошибка имя поля уже существует")
    void failCreateFieldWithSameName() throws Exception {
        createGeneralTable();
        TField newFieldWithDependence = new TField("value", Double.class);
        Assertions.assertThatThrownBy(() -> schema.createField(newFieldWithDependence, "general", "org.infomaximum.rocksdb"))
                .isExactlyInstanceOf(FieldAlreadyExistsException.class);
    }

    //Удаление полей_____________________________________________
    @Test
    @DisplayName("Удаляет поле")
    void removeField() throws Exception {
        Table table = createExchangeFolderTable();
        schema.dropField("email", table.getName(), table.getNamespace());

        List<TField> expectedFields = new ArrayList<>(table.getFields());
        expectedFields.remove(new TField("email", String.class));
        List<THashIndex> hashIndexes = new ArrayList<>(table.getHashIndexes());
        hashIndexes.remove(new THashIndex("email", "uuid"));
        Table expectedTable = new Table(table.getName(), table.getNamespace(), expectedFields, hashIndexes, table.getPrefixIndexes(), table.getIntervalIndexes(), table.getRangeIndexes());

        assertThatSchemaContainsTable(expectedTable);
        domainObjectSource.executeTransactional(transaction -> {
            try(IteratorEntity<ExchangeFolderReadable> it = transaction.find(ExchangeFolderReadable.class, EmptyFilter.INSTANCE)) {
                Assertions.assertThat(it.hasNext()).isTrue();
                while (it.hasNext()) {
                    ExchangeFolderReadable ef = it.next();
                    Assertions.assertThat(ef.getUserEmail()).isNull();
                }
            }
        });
    }

    @Test
    @DisplayName("Удаляет зависимое поле")
    void removeForeignDependencyField() throws Exception {
        Table table = createExchangeFolderTable();
        schema.dropField("parent_id", table.getName(), table.getNamespace());

        List<TField> expectedFields = new ArrayList<>(table.getFields());
        expectedFields.remove(new TField("parent_id", new TableReference(table.getName(), table.getNamespace())));
        List<THashIndex> hashIndexes = new ArrayList<>(table.getHashIndexes());
        hashIndexes.remove(new THashIndex("parent_id"));
        Table expectedTable = new Table(table.getName(), table.getNamespace(), expectedFields, hashIndexes, table.getPrefixIndexes(), table.getIntervalIndexes(), table.getRangeIndexes());

        assertThatSchemaContainsTable(expectedTable);
        domainObjectSource.executeTransactional(transaction -> {
            try(IteratorEntity<ExchangeFolderReadable> it = transaction.find(ExchangeFolderReadable.class, EmptyFilter.INSTANCE)) {
                Assertions.assertThat(it.hasNext()).isTrue();
                while (it.hasNext()) {
                    ExchangeFolderReadable ef = it.next();
                    Assertions.assertThat(ef.getParentId()).isNull();
                }
            }
        });
    }

    //Переименование полей_____________________________________________
    @Test
    @DisplayName("Переименование поля таблицы")
    void renameTableFieldTest() throws Exception {
        Table exchangeFolderTable = createExchangeFolderTable();
        schema.renameField("state", "newValue", "ExchangeFolder", "org.infomaximum.exchange");

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("uuid", String.class));
            add(new TField("email", String.class));
            add(new TField("date", Instant.class));
            add(new TField("newValue", String.class));
            add(new TField("parent_id", new TableReference("ExchangeFolder", "org.infomaximum.exchange")));
        }};
        exchangeFolderTable = new Table(exchangeFolderTable.getName(), exchangeFolderTable.getNamespace(), fields, exchangeFolderTable.getHashIndexes());
        assertThatSchemaContainsTable(exchangeFolderTable);
    }

    @Test
    @DisplayName("Переименование поля таблицы у которого есть индекс")
    void renameTableFieldWithIndexTest() throws Exception {
        Table exchangeFolderTable = createExchangeFolderTable();
        schema.renameField("email", "znew", "ExchangeFolder", "org.infomaximum.exchange");

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("uuid", String.class));
            add(new TField("znew", String.class));
            add(new TField("date", Instant.class));
            add(new TField("state", String.class));
            add(new TField("parent_id", new TableReference("ExchangeFolder", "org.infomaximum.exchange")));
        }};
        List<THashIndex> expectedHashIndexes = new ArrayList<>(exchangeFolderTable.getHashIndexes());
        expectedHashIndexes.remove(1);
        expectedHashIndexes.add(new THashIndex("uuid", "znew"));
        exchangeFolderTable = new Table(exchangeFolderTable.getName(), exchangeFolderTable.getNamespace(), fields, expectedHashIndexes);
        assertThatSchemaContainsTable(exchangeFolderTable);
    }

    @Test
    @DisplayName("Ошибка переименования поля таблицы. Поле с таким именем уже существует")
    void failRenameTableFieldNameAlreadyExistsTest() throws Exception {
        createExchangeFolderTable();
        Assertions.assertThatThrownBy(() -> schema.renameField("state", "email", "ExchangeFolder", "org.infomaximum.exchange"))
                .isExactlyInstanceOf(FieldAlreadyExistsException.class);
    }

    //Удаление индексов_____________________________________________
    @Test
    @DisplayName("Удаляет hash index из таблицы с одним хэшем")
    void removeSimpleIndexTest() throws Exception {
        Table table = createGeneralTable();

        THashIndex removingHashIndex = new THashIndex("value");
        schema.dropIndex(removingHashIndex, table.getName(), table.getNamespace());

        List<THashIndex> expectedHashes = new ArrayList<>(table.getHashIndexes());
        expectedHashes.remove(removingHashIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                expectedHashes,
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatNoAnyRecords(GeneralReadable.class, new HashFilter(GeneralEditable.FIELD_VALUE, 12L));
    }

    @Test
    @DisplayName("Удаляет index'ы из таблицы")
    void removeIndexTest() throws Exception {
        createExchangeFolderTable();
        Table table = createStoreFolderTable();

        THashIndex removingHashIndex = new THashIndex("name");
        schema.dropIndex(removingHashIndex, table.getName(), table.getNamespace());

        List<THashIndex> expectedHashes = new ArrayList<>(table.getHashIndexes());
        expectedHashes.remove(removingHashIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                expectedHashes,
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatNoAnyRecords(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName"));
        domainObjectSource.executeTransactional(transaction -> {
            try(IteratorEntity<StoreFileReadable> si = transaction.
                    find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName")
                            .appendField(StoreFileReadable.FIELD_SIZE, 12L))) {
                Assertions.assertThat(si.hasNext()).isTrue();
            }
        });
    }

    @Test
    @DisplayName("Удаляет hash index, состоящий из нескольких полей, из таблицы")
    void removeMultiIndexTest() throws Exception {
        createExchangeFolderTable();
        Table table = createStoreFolderTable();

        THashIndex removingHashIndex = new THashIndex("size", "name");
        schema.dropIndex(removingHashIndex, table.getName(), table.getNamespace());

        List<THashIndex> expectedHashes = new ArrayList<>(table.getHashIndexes());
        expectedHashes.remove(new THashIndex("name", "size"));
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                expectedHashes,
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatNoAnyRecords(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName").appendField(StoreFileReadable.FIELD_SIZE, 12L));
    }

    @Test
    @DisplayName("Удаляет hash index из таблицы и заного его добавляет")
    void removeAndAttachIndexTest() throws Exception {
        createExchangeFolderTable();
        Table table = createStoreFolderTable();

        THashIndex removingHashIndex = new THashIndex("name");
        schema.dropIndex(removingHashIndex, table.getName(), table.getNamespace());
        assertThatNoAnyRecords(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName"));

        schema.createIndex(removingHashIndex, "StoreFile", "org.infomaximum.store");
        List<THashIndex> expectedHashes = new ArrayList<>(table.getHashIndexes());
        expectedHashes.remove(removingHashIndex);
        expectedHashes.add(removingHashIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                expectedHashes,
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatHasAnyRecords(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName"));
    }

    @Test
    @DisplayName("Удаляет hash index из таблицы и заного его добавляет")
    void removeAndAttachPrefixIndexTest() throws Exception {
        createExchangeFolderTable();
        Table table = createStoreFolderTable();

        TPrefixIndex removingIndex = new TPrefixIndex("name");
        schema.dropIndex(removingIndex, table.getName(), table.getNamespace());
        List<TPrefixIndex> expectedHashes = new ArrayList<>(table.getPrefixIndexes());
        expectedHashes.remove(removingIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                table.getHashIndexes(),
                expectedHashes,
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatNoAnyRecords(StoreFileReadable.class, new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName"));

        schema.createIndex(removingIndex, "StoreFile", "org.infomaximum.store");
        expectedHashes = new ArrayList<>(table.getPrefixIndexes());
        expectedHashes.remove(removingIndex);
        expectedHashes.add(removingIndex);
        expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                table.getHashIndexes(),
                expectedHashes,
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatHasAnyRecords(StoreFileReadable.class, new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName"));
    }

    @Test
    @DisplayName("Удаляет interval index из таблицы и заного его добавляет")
    void removeAndAttachIntervalIndexTest() throws Exception {
        createExchangeFolderTable();
        Table table = createStoreFolderTable();

        TIntervalIndex removingIndex = new TIntervalIndex("size");
        schema.dropIndex(removingIndex, table.getName(), table.getNamespace());
        List<TIntervalIndex> expectedHashes = new ArrayList<>(table.getIntervalIndexes());
        expectedHashes.remove(removingIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                table.getHashIndexes(),
                table.getPrefixIndexes(),
                expectedHashes,
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatNoAnyRecords(StoreFileReadable.class, new IntervalFilter(StoreFileReadable.FIELD_SIZE, 1L, 13L));

        schema.createIndex(removingIndex, "StoreFile", "org.infomaximum.store");
        expectedHashes = new ArrayList<>(table.getIntervalIndexes());
        expectedHashes.remove(removingIndex);
        expectedHashes.add(removingIndex);
        expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                table.getHashIndexes(),
                table.getPrefixIndexes(),
                expectedHashes,
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatHasAnyRecords(StoreFileReadable.class, new IntervalFilter(StoreFileReadable.FIELD_SIZE, 1L, 13L));
    }

    @Test
    @DisplayName("Удаляет range index из таблицы и заного его добавляет")
    void removeAndAttachRangeIndexTest() throws Exception {
        createExchangeFolderTable();
        Table table = createStoreFolderTable();

        TRangeIndex removingIndex = new TRangeIndex("begin", "end");
        schema.dropIndex(removingIndex, table.getName(), table.getNamespace());
        List<TRangeIndex> expectedHashes = new ArrayList<>(table.getRangeIndexes());
        expectedHashes.remove(removingIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                table.getHashIndexes(),
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                expectedHashes);
        assertThatSchemaContainsTable(expected);
        assertThatNoAnyRecords(StoreFileReadable.class, new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN, StoreFileReadable.FIELD_END), 9L, 14L));

        schema.createIndex(removingIndex, "StoreFile", "org.infomaximum.store");
        expectedHashes = new ArrayList<>(table.getRangeIndexes());
        expectedHashes.remove(removingIndex);
        expectedHashes.add(removingIndex);
        expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                table.getHashIndexes(),
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                expectedHashes);
        assertThatSchemaContainsTable(expected);
        assertThatHasAnyRecords(StoreFileReadable.class, new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN, StoreFileReadable.FIELD_END), 9L, 14L));
    }

    private Table createGeneralTable() throws Exception {
        Schema.resolve(GeneralReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("value", Long.class));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("value"));
        }};
        Table table = new Table("general", "org.infomaximum.rocksdb", fields, hashIndexes);
        schema.createTable(table);
        domainObjectSource.executeTransactional(transaction -> {
            GeneralEditable generalEditable = transaction.create(GeneralEditable.class);
            generalEditable.setValue(12L);
            transaction.save(generalEditable);

            generalEditable = transaction.create(GeneralEditable.class);
            generalEditable.setValue(111L);
            transaction.save(generalEditable);

            generalEditable = transaction.create(GeneralEditable.class);
            generalEditable.setValue(12L);
            transaction.save(generalEditable);
        });
        return schema.getTable("general", "org.infomaximum.rocksdb");
    }

    private Table createExchangeFolderTable() throws Exception {
        Schema.resolve(ExchangeFolderReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("uuid", String.class));
            add(new TField("email", String.class));
            add(new TField("date", Instant.class));
            add(new TField("state", String.class));
            add(new TField("parent_id", new TableReference("ExchangeFolder", "org.infomaximum.exchange")));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("email", "uuid"));
        }};
        Table table = new Table("ExchangeFolder", "org.infomaximum.exchange", fields, hashIndexes);
        schema.createTable(table);
        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable exchangeFolderEditable1 = transaction.create(ExchangeFolderEditable.class);
            exchangeFolderEditable1.setUuid("uuid1");
            exchangeFolderEditable1.setUserEmail("email1");
            exchangeFolderEditable1.setSyncDate(Instant.now());
            exchangeFolderEditable1.setSyncState("adsf");
            transaction.save(exchangeFolderEditable1);

            ExchangeFolderEditable exchangeFolderEditable2 = transaction.create(ExchangeFolderEditable.class);
            exchangeFolderEditable2.setUuid("uuid2");
            exchangeFolderEditable2.setUserEmail("email2");
            exchangeFolderEditable2.setSyncDate(Instant.now());
            exchangeFolderEditable2.setSyncState("adsf2");
            exchangeFolderEditable2.setParentId(1L);
            transaction.save(exchangeFolderEditable2);

            ExchangeFolderEditable exchangeFolderEditable3 = transaction.create(ExchangeFolderEditable.class);
            exchangeFolderEditable3.setUuid("uuid3");
            exchangeFolderEditable3.setUserEmail("email3");
            exchangeFolderEditable3.setSyncDate(Instant.now());
            exchangeFolderEditable3.setSyncState("adsf3");
            exchangeFolderEditable3.setParentId(2L);
            transaction.save(exchangeFolderEditable3);
        });

        return schema.getTable("ExchangeFolder", "org.infomaximum.exchange");
    }

    private Table createStoreFolderTable() throws Exception {
        Schema.resolve(StoreFileReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("name", String.class));
            add(new TField("type", String.class));
            add(new TField("size", Long.class));
            add(new TField("single", Boolean.class));
            add(new TField("format", String.class));
            add(new TField("folder_id", new TableReference("ExchangeFolder", "org.infomaximum.exchange")));
            add(new TField("double", Double.class));
            add(new TField("begin_time", Instant.class));
            add(new TField("end_time", Instant.class));
            add(new TField("begin", Long.class));
            add(new TField("end", Long.class));
            add(new TField("local_begin", LocalDateTime.class));
            add(new TField("local_end", LocalDateTime.class));
            add(new TField("data", byte[].class));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("size"));
            add(new THashIndex("name"));
            add(new THashIndex("size", "name"));
            add(new THashIndex("format"));
            add(new THashIndex("local_begin"));
        }};
        List<TPrefixIndex> prefixIndexes = new ArrayList<TPrefixIndex>() {{
            add(new TPrefixIndex("name"));
            add(new TPrefixIndex("name", "type"));
        }};
        List<TIntervalIndex> intervalIndexes = new ArrayList<TIntervalIndex>() {{
            add(new TIntervalIndex("size"));
            add(new TIntervalIndex("double"));
            add(new TIntervalIndex("begin_time"));
            add(new TIntervalIndex("local_begin"));
            add(new TIntervalIndex("size", new String[]{"name"}));
            add(new TIntervalIndex("size", new String[]{"folder_id"}));
        }};
        List<TRangeIndex> rangeIndexes = new ArrayList<TRangeIndex>() {{
            add(new TRangeIndex("begin", "end"));
            add(new TRangeIndex("begin", "end", new String[]{"folder_id"}));
            add(new TRangeIndex("begin_time", "end_time"));
            add(new TRangeIndex("begin_time", "begin_time", new String[]{"folder_id"}));
            add(new TRangeIndex("local_begin", "local_end"));
        }};
        Table table = new Table("StoreFile",
                "org.infomaximum.store",
                fields,
                hashIndexes,
                prefixIndexes,
                intervalIndexes,
                rangeIndexes
        );
        schema.createTable(table);

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(12L);
            storeFileEditable.setBegin(12L);
            storeFileEditable.setEnd(14L);
            storeFileEditable.setFileName("FileName");
            storeFileEditable.setDouble(12.4);
            transaction.save(storeFileEditable);

            storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(11L);
            storeFileEditable.setBegin(1L);
            storeFileEditable.setEnd(2L);
            storeFileEditable.setFileName("FileName2");
            storeFileEditable.setDouble(13.4);
            transaction.save(storeFileEditable);

            storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(1L);
            storeFileEditable.setBegin(10L);
            storeFileEditable.setEnd(13L);
            storeFileEditable.setFileName("FileName");
            storeFileEditable.setDouble(13.123);
            transaction.save(storeFileEditable);
        });
        return schema.getTable("StoreFile", "org.infomaximum.store");
    }

    private void assertThatSchemaContainsTable(Table expected) throws DatabaseException {
        Schema schema = Schema.read(rocksDBProvider);
        Table actual = schema.getTable(expected.getName(), expected.getNamespace());
        Assertions.assertThat(actual).isEqualTo(expected);
        String dataColumnFamily = expected.getNamespace() + "." + expected.getName();
        String indexColumnFamily = expected.getNamespace() + "." + expected.getName() + ".index";
        Assertions.assertThat(rocksDBProvider.containsColumnFamily(dataColumnFamily)).isTrue();
        Assertions.assertThat(rocksDBProvider.containsColumnFamily(indexColumnFamily)).isTrue();
    }

    private void assertThatTableIsEmpty(String name, String namespace) throws DatabaseException {
        Schema schema = Schema.read(rocksDBProvider);
        DBTable table = schema.getDbSchema().getTable(name, namespace);

        try (DBIterator it = rocksDBProvider.createIterator(table.getDataColumnFamily())){
            KeyValue kv = it.seek(null);
            Assertions.assertThat(kv)
                    .as("Table %s.%s doesn't empty", namespace, name)
                    .isNull();
        }

        try (DBIterator it = rocksDBProvider.createIterator(table.getIndexColumnFamily())){
            KeyValue kv = it.seek(null);
            Assertions.assertThat(kv)
                    .as("Table %s.%s doesn't empty. Indexes doesn't empty", namespace, name)
                    .isNull();
        }
    }

    private void assertThatTableIsEmpty(Table table) throws DatabaseException {
        assertThatTableIsEmpty(table.getName(), table.getNamespace());
    }

    private void assertTableDoesntExist(String name, String namespace) throws DatabaseException {
        Schema schema = Schema.read(rocksDBProvider);
        Assertions.assertThatThrownBy(() -> schema.getTable(name, namespace)).isExactlyInstanceOf(TableNotFoundException.class);
        String dataColumnFamily = namespace + "." + name;
        String indexColumnFamily = namespace + "." + name + ".index";
        Assertions.assertThat(rocksDBProvider.containsColumnFamily(dataColumnFamily)).isFalse();
        Assertions.assertThat(rocksDBProvider.containsColumnFamily(indexColumnFamily)).isFalse();
    }

    private <T extends DomainObject> void assertThatNoAnyRecords(Class<T> dbEntity, Filter filter) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            try(IteratorEntity<T> it = transaction.find(dbEntity, filter)) {
                Assertions.assertThat(it.hasNext()).isFalse();
            }
        });
    }

    private <T extends DomainObject> void assertThatHasAnyRecords(Class<T> dbEntity, Filter filter) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            try(IteratorEntity<T> it = transaction.find(dbEntity, filter)) {
                Assertions.assertThat(it.hasNext()).isTrue();
            }
        });
    }
}
