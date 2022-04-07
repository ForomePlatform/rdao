package org.forome.database;

import org.forome.database.domainobject.DomainObjectSource;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.database.exception.UnexpectedEndObjectException;
import org.forome.database.provider.DBTransaction;
import org.forome.database.schema.Schema;
import org.forome.database.utils.TypeConvert;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;


public class DataCommandTest extends StoreFileDataTest {

    //All iterator test
    @Test
    public void selectAllIterator() throws Exception {
        final int insertedRecordCount = 10;
        Collection<? extends DomainObject> expected = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource.select("StoreFile", "org.infomaximum.store")){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, expected);
        }
    }

    @Test()
    public void selectAllIteratorNoneObjects() {
        try (RecordIterator iterator = recordSource.select("StoreFile", "org.infomaximum.store")){
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    public void checkInnerStructure() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
            transaction.delete(Schema.getEntity(StoreFileReadable.class).getColumnFamily(), TypeConvert.pack(2L));
            transaction.commit();
        }

        try (RecordIterator iterator = recordSource.select("StoreFile", "org.infomaximum.store")){
            Assertions.assertThatThrownBy(() -> {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }).isInstanceOf(UnexpectedEndObjectException.class);
        }
    }

    @Test
    public void orderingIterate() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource.select("StoreFile", "org.infomaximum.store")){
            int iteratedRecordCount = 0;
            long prevId = 0;
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();

                if (prevId == storeFile.getId()) Assertions.fail("Fail next object");
                if (prevId >= storeFile.getId()) Assertions.fail("Fail sort id to iterators");
                prevId = storeFile.getId();
                ++iteratedRecordCount;
            }
            Assertions.assertThat(insertedRecordCount).isEqualTo(iteratedRecordCount);
        }
    }

    @Test
    public void iterateTransactional() throws Exception {
        recordSource.executeTransactional(transaction ->  {
            // insert
            long objId = transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                            new String[]{"size"},
                            new Object[]{10L});

            Assertions.assertThat(transaction.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, objId))
                    .matches(record -> record.getValues()[StoreFileReadable.FIELD_SIZE].equals(10L));

            // change
            transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    objId,
                    new String[]{"size"},
                    new Object[]{20L});

            try (RecordIterator i = transaction.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE)) {
                Assertions.assertThat(i.next().getValues()[StoreFileReadable.FIELD_SIZE]).isEqualTo(20L);
            }
        });
    }

    @Test
    public void iterateAndChange() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        recordSource.executeTransactional(transaction ->  {
            try (RecordIterator i = transaction.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE)) {
                while (i.hasNext()) {
                    Record current = i.next();
                    if (current.getId() == insertedRecordCount) {
                        break;
                    }

                    Record newNext = transaction.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, current.getId() + 1);
                    transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                            newNext.getId(),
                            new String[]{"double", "name"},
                            new Object[]{Double.POSITIVE_INFINITY, UUID.randomUUID().toString()});

                    Record next = i.next();
                    Assertions.assertThat(next.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isEqualTo("name");
                    Assertions.assertThat(next.getValues()[StoreFileReadable.FIELD_DOUBLE]).isNull();

                }
            }
        });
    }

    @Test
    public void removeAndFind() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);


        recordSource.executeTransactional(transaction ->  {
            transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1);
            transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 5);

            Assertions.assertThat(transaction.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1)).isNull();
        });

        Assertions.assertThat(recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1)).isNull();
    }

    //Hash iterator test
    @Test
    @DisplayName("Проверка HashIndex итератора. Находит ВСЕ StoreFiles объекты по заданному HashIndex для одного поля")
    public void selectHashIteratorForAll() throws Exception {
        final int insertedRecordCount = 10;
        Collection<? extends DomainObject> expected = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, expected);
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Находит только один StoreFiles объекты по заданному HashIndex для одного поля")
    public void selectHashIteratorOnlyOne() throws Exception {
        final int insertedRecordCount = 10;
        List<? extends DomainObject> dbData = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SIZE, 2L))){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, dbData.get(2));
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Находит некоторые StoreFiles объекты по заданному HashIndex для двух полей")
    public void selectHashIteratorTwoFields() throws Exception {
        final int insertedRecordCount = 10;
        List<? extends DomainObject> dbData = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SIZE, 2L)
                        .appendField(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, dbData.get(2));
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Находит ВСЕ StoreFiles объекты по заданному HashIndex для двух полей")
    public void selectHashIteratorTwoFieldsAllMatch() throws Exception {
        final int insertedRecordCount = 10;
        List<? extends DomainObject> expected = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SINGLE, true)
                        .appendField(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, expected);
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Не находит объекты по заданному HashIndex")
    public void selectHashIteratorTwoFieldsNoneMatch() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SINGLE, false)
                        .appendField(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }
    }


    @Test
    @DisplayName("Проверка HashIndex итератора. Не должен упасть с NPE при отстутствии объектов в бд")
    public void selectHashIteratorNoneObjects() {
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SINGLE, false)
                        .appendField(StoreFileReadable.FIELD_FILE_NAME, "name"))) {
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Не должен упасть с NPE при удалении ключа с id 2 [00 00 00 00 00 00 00 02]")
    public void checkInnerStructureHashIterator() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
            transaction.delete(Schema.getEntity(StoreFileReadable.class).getColumnFamily(), TypeConvert.pack(2L));
            transaction.commit();
        }
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name"))) {
            while (iterator.hasNext()) {
                iterator.next();
            }
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Проверка порядка id")
    public void orderingIterateHashIndex() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            int iteratedRecordCount = 0;
            long prevId = 0;
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();

                if (prevId == storeFile.getId()) Assertions.fail("Fail next object");
                if (prevId >= storeFile.getId()) Assertions.fail("Fail sort id to iterators");
                prevId = storeFile.getId();
                ++iteratedRecordCount;
            }
            Assertions.assertThat(insertedRecordCount).isEqualTo(iteratedRecordCount);
        }
    }


    private List<? extends DomainObject> initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws Exception {
        List<StoreFileReadable> result = new ArrayList<>();
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(i);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
//                obj.setFormat(FormatType.B);
                transaction.save(obj);
                result.add(obj);
            }
        });
        return result;
    }
}
