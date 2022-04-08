package org.forome.database.domainobject.engine;

import org.forome.database.Record;
import org.forome.database.RecordIterator;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.database.domainobject.filter.IntervalFilter;
import org.forome.database.domainobject.filter.PrefixFilter;
import org.forome.database.exception.ForeignDependencyException;
import org.forome.database.maintenance.ChangeMode;
import org.forome.database.maintenance.DomainService;
import org.forome.database.schema.Schema;
import org.forome.database.utils.TypeConvert;
import org.forome.domain.ExchangeFolderEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.domain.*;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.Test;


public class TransactionTest extends StoreFileDataTest {

    @Test
    public void optimisticTransactionLazyTest() throws Exception {
        String fileName = "aaa.txt";
        long size = 15L;

        recordSource.executeTransactional(transaction -> {
            long id = transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"name", "size"}, new Object[] {fileName, size});

            try (RecordIterator ie = transaction.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE)) {
                Record storeFile = ie.next();

                Assert.assertEquals(fileName, storeFile.getValues()[StoreFileReadable.FIELD_FILE_NAME]);
                Assert.assertEquals(size, storeFile.getValues()[StoreFileReadable.FIELD_SIZE]);
            }
        });
    }

    @Test
    public void create() throws Exception {
        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        String fileName="application/json";
        String contentType="info.json";
        long size=1000L;

        recordSource.executeTransactional(transaction -> transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new String[]{"name", "size", "type"}, new Object[] {fileName, size, contentType}));

        //Загружаем сохраненый объект
        Record storeFileCheckSave = recordSource.executeFunctionTransactional(transaction -> transaction.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1));
        Assertions.assertThat(storeFileCheckSave).isNotNull();
        Assertions.assertThat(fileName).isEqualTo(storeFileCheckSave.getValues()[StoreFileReadable.FIELD_FILE_NAME]);
        Assertions.assertThat(contentType).isEqualTo(storeFileCheckSave.getValues()[StoreFileReadable.FIELD_CONTENT_TYPE]);
        Assertions.assertThat(size).isEqualTo(storeFileCheckSave.getValues()[StoreFileReadable.FIELD_SIZE]);
    }

    @Test
    public void update() throws Exception {
        recordSource.executeTransactional(transaction -> transaction.insertRecord(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE,
                new String[]{}, new Object[]{}));
        long recordId = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new String[]{}, new Object[]{}));
        Record obj = recordSource.executeFunctionTransactional(transaction -> transaction.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1));
        Assertions.assertThat(obj).isNotNull();
        Assertions.assertThat(obj.getValues()).containsOnlyNulls();
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new IntervalFilter(StoreFileReadable.FIELD_DOUBLE, 0d, 1d)).hasNext()).isTrue();

        //Добавляем объект
        recordSource.executeFunctionTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                recordId,
                new String[]{"name", "folder_id", "single", "data", "double"},
                new Object[]{"test", 1L, false, new byte[]{1,2}, 0.1}));

        //Загружаем сохраненый объект
        obj = recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
        Assertions.assertThat(obj).isNotNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isEqualTo("test");
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FOLDER_ID]).isEqualTo(1L);
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_SINGLE]).isEqualTo(false);
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DATA]).isEqualTo(new byte[]{1,2});
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DOUBLE]).isEqualTo(0.1);
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "test")).hasNext()).isTrue();
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "test").appendField(StoreFileReadable.FIELD_SINGLE, false)).hasNext()).isTrue();
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new IntervalFilter(StoreFileReadable.FIELD_DOUBLE, 0d, 1d)).hasNext()).isTrue();

        //Редактируем сохраненный объект
        recordSource.executeTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                recordId,
                new String[]{"name", "folder_id", "single", "data", "double"},
                new Object[]{null, null, null, null, null}));
        obj = recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
        Assertions.assertThat(obj).isNotNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FOLDER_ID]).isNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_SINGLE]).isNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DATA]).isNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DOUBLE]).isNull();

        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "test")).hasNext()).isFalse();
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new HashFilter(StoreFileReadable.FIELD_FILE_NAME, null)).hasNext()).isTrue();
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "test").appendField(StoreFileReadable.FIELD_SINGLE, false)).hasNext()).isFalse();
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new HashFilter(StoreFileReadable.FIELD_FILE_NAME, null).appendField(StoreFileReadable.FIELD_SINGLE, null)).hasNext()).isTrue();
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new HashFilter(StoreFileReadable.FIELD_FILE_NAME, null).appendField(StoreFileReadable.FIELD_SIZE, null)).hasNext()).isTrue();
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "test")).hasNext()).isFalse();
        Assertions.assertThat(recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new IntervalFilter(StoreFileReadable.FIELD_DOUBLE, 0d, 1d)).hasNext()).isTrue();


        //Повторно редактируем сохраненный объект
        recordSource.executeTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                recordId,
                new String[]{"name", "data"},
                new Object[]{"", new byte[] {TypeConvert.NULL_BYTE_ARRAY_SCHIELD, 1, 2}}));
        obj = recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
        Assertions.assertThat(obj).isNotNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isEqualTo("");
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DATA]).isEqualTo(new byte[] {TypeConvert.NULL_BYTE_ARRAY_SCHIELD, 1, 2});
    }

    @Test
    public void updateByNonExistenceObject() {
        Assertions.assertThatThrownBy(() ->
                recordSource.executeTransactional(transaction -> transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                        new String[]{"folder_id"}, new Object[] {256L}))).isInstanceOf(ForeignDependencyException.class);
    }

    @Test
    public void updateValueStringEmptyThenNull() throws Exception {
        final String emptyFileName = "";
        final String contentType = "info.json";

        //Добавляем объект
        long recordId = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                        new String[]{"type", "name"}, new Object[]{contentType, emptyFileName}));

        //Загружаем сохраненый объект
        Record storeFileCheckSave = recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
        Assertions.assertThat(storeFileCheckSave.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isEqualTo(emptyFileName);
        Assertions.assertThat(storeFileCheckSave.getValues()[StoreFileReadable.FIELD_CONTENT_TYPE]).isEqualTo(contentType);

        //Редактируем сохраненый объект
        recordSource.executeFunctionTransactional(transaction ->
                transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                        recordId,
                        new String[]{"type"}, new Object[]{null}));

        //Загружаем сохраненый объект
        Record storeFileCheckEdit = recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
        Assertions.assertThat(storeFileCheckEdit.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isEqualTo(emptyFileName);
        Assertions.assertThat(storeFileCheckEdit.getValues()[StoreFileReadable.FIELD_CONTENT_TYPE]).isNull();
    }

    @Test
    public void saveEmptyDomainObject() throws Exception {
        final String emptyFileName = "";
        final String contentType = "info.json";

        //Добавляем объект
        long recordId = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                        new String[]{"type", "name"}, new Object[]{contentType, emptyFileName}));
        //Загружаем сохраненый объект и сразу без редактирования полей вызываем сохранение
        recordSource.executeTransactional(transaction -> {
                Record record = transaction.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
                transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                        record);
        });
    }

    @Test
    public void removeOneObject() throws Exception {
        //Добавляем объект
        long recordId1 = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                        new String[]{}, new Object[]{}));
        long recordId2 = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                        new String[]{}, new Object[]{}));
        long recordId3 = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                        new String[]{}, new Object[]{}));

        //Проверяем что файлы сохранены
        Assertions.assertThat(recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId1)).isNotNull();
        Assertions.assertThat(recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId2)).isNotNull();
        Assertions.assertThat(recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId3)).isNotNull();

        //Удаляем 2-й объект
        recordSource.executeTransactional(transaction -> transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId2));

        //Проверяем, корректность удаления
        Assertions.assertThat(recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId1)).isNotNull();
        Assertions.assertThat(recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId2)).isNull();
        Assertions.assertThat(recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId3)).isNotNull();
    }

    @Test
    public void removeReferencedObject() throws Exception {
        long folderId = recordSource.executeFunctionTransactional(transaction -> transaction.insertRecord(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE,
                new String[]{}, new Object[]{}));
        long recordId = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                        new String[]{"folder_id"}, new Object[]{folderId}));

        Assertions.assertThatThrownBy(() -> recordSource
                        .executeTransactional(transaction -> transaction.deleteRecord(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE, folderId)))
                .isInstanceOf(ForeignDependencyException.class);

        recordSource
                .executeTransactional(transaction -> {
                    transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
                    transaction.deleteRecord(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE, folderId);
                });
    }

    @Test
    public void removeAll() throws Exception {
        recordSource.executeTransactional(transaction -> {
            long folderId = transaction.insertRecord(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE, new String[]{}, new Object[]{});
            long folderId2 = transaction.insertRecord(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE, new String[]{}, new Object[]{});

            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"folder_id"}, new Object[]{null});

            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"folder_id"}, new Object[]{null});

            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"folder_id"}, new Object[]{null});
        });

        recordSource.executeTransactional(transaction -> transaction.clearTable(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE));
        Assertions.assertThat(recordSource.select(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE).hasNext()).isFalse();

        new DomainService(domainObjectSource.getDbProvider(), schema)
                .setChangeMode(ChangeMode.NONE)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(ExchangeFolderEditable.class))
                .execute();

        recordSource.executeTransactional(transaction -> {
            long folderId = transaction.insertRecord(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE, new String[]{}, new Object[]{});
            long folderId2 = transaction.insertRecord(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE, new String[]{}, new Object[]{});

            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"folder_id"}, new Object[]{null});

            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"folder_id"}, new Object[]{folderId2});

            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"folder_id"}, new Object[]{folderId2});

            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"folder_id"}, new Object[]{null});
        });

        Assertions.assertThatThrownBy(() -> recordSource
                        .executeTransactional(transaction -> transaction.clearTable(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE)))
                .isInstanceOf(ForeignDependencyException.class);

        recordSource.executeTransactional(transaction -> {
            transaction.clearTable(STORE_FILE_NAME, STORE_FILE_NAMESPACE);
            transaction.clearTable(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE);
        });
    }
}
