package org.forome.database.domainobject;

import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.domainobject.filter.EmptyFilter;
import org.forome.database.exception.ForeignDependencyException;
import org.forome.database.maintenance.ChangeMode;
import org.forome.database.maintenance.DomainService;
import org.forome.database.schema.Schema;
import org.forome.database.utils.TypeConvert;
import org.forome.domain.ExchangeFolderEditable;
import org.forome.domain.ExchangeFolderReadable;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.domain.type.FormatType;
import org.junit.Assert;
import org.junit.Test;

public class TransactionTest extends StoreFileDataTest {

    @Test
    public void optimisticTransactionLazyTest() throws Exception {
        String fileName = "aaa.txt";
        long size = 15L;

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile1 = transaction.create(StoreFileEditable.class);
            storeFile1.setFileName(fileName);
            storeFile1.setSize(size);
            transaction.save(storeFile1);

            try (IteratorEntity<StoreFileReadable> ie = transaction.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
                StoreFileReadable storeFile2 = ie.next();

                Assert.assertEquals(fileName, storeFile2.getFileName());
                Assert.assertEquals(size, storeFile2.getSize());
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
        FormatType format = FormatType.B;

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
            storeFile.setContentType(contentType);
            storeFile.setFileName(fileName);
            storeFile.setSize(size);
            storeFile.setFormat(format);
            transaction.save(storeFile);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckSave = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(fileName, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave.getContentType());
        Assert.assertEquals(size, storeFileCheckSave.getSize());
        Assert.assertEquals(format, storeFileCheckSave.getFormat());
    }

    @Test
    public void save() throws Exception {
        domainObjectSource.executeTransactional(transaction -> transaction.save(transaction.create(StoreFileEditable.class)));
        StoreFileReadable obj = domainObjectSource.get(StoreFileReadable.class, 1);
        Assert.assertNotNull(obj);
        Assert.assertNull(obj.getFileName());
        Assert.assertNull(obj.getFolderId());
        Assert.assertNull(obj.getFormat());
        Assert.assertNull(obj.isSingle());
        Assert.assertArrayEquals(null, obj.getData());
        Assert.assertNull(obj.getDouble());

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            transaction.setForeignFieldEnabled(false);

            StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1);
            storeFile.setFileName("test");
            storeFile.setFolderId(1);
            storeFile.setFormat(FormatType.A);
            storeFile.setSingle(false);
            storeFile.setData(new byte[]{1,2});
            storeFile.setDouble(0.1);
            transaction.save(storeFile);
        });

        //Загружаем сохраненый объект
        obj = domainObjectSource.get(StoreFileReadable.class, 1);
        Assert.assertNotNull(obj);
        Assert.assertEquals("test", obj.getFileName());
        Assert.assertEquals(Long.valueOf(1), obj.getFolderId());
        Assert.assertEquals(FormatType.A, obj.getFormat());
        Assert.assertEquals(false, obj.isSingle());
        Assert.assertArrayEquals(new byte[]{1,2}, obj.getData());
        Assert.assertEquals(Double.valueOf(0.1), obj.getDouble());

        //Редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1);
            storeFile.setFileName(null);
            storeFile.setFolderId(null);
            storeFile.setFormat(null);
            storeFile.setSingle(null);
            storeFile.setData(null);
            storeFile.setDouble(null);
            transaction.save(storeFile);
        });
        obj = domainObjectSource.get(StoreFileReadable.class, 1);
        Assert.assertNotNull(obj);
        Assert.assertNull(obj.getFileName());
        Assert.assertNull(obj.getFolderId());
        Assert.assertNull(obj.getFormat());
        Assert.assertNull(obj.isSingle());
        Assert.assertArrayEquals(null, obj.getData());
        Assert.assertNull(obj.getDouble());


        //Повторно редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1);
            storeFile.setFileName("");
            storeFile.setData(new byte[] {TypeConvert.NULL_BYTE_ARRAY_SCHIELD, 1, 2});
            transaction.save(storeFile);
        });


        obj = domainObjectSource.get(StoreFileReadable.class, 1);
        Assert.assertNotNull(obj);
        Assert.assertEquals("", obj.getFileName());
        Assert.assertArrayEquals(new byte[] {TypeConvert.NULL_BYTE_ARRAY_SCHIELD, 1, 2}, obj.getData());
    }

    @Test
    public void updateByNonExistenceObject() throws Exception {
        try {
            domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable file = transaction.create(StoreFileEditable.class);
                file.setFolderId(256);
                transaction.save(file);
            });
            Assert.fail();
        } catch (ForeignDependencyException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void updateValueStringEmptyThenNull() throws Exception {
        final long objectId = 1;
        final String emptyFileName = "";
        final String contentType = "info.json";

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
            storeFile.setContentType(contentType);
            storeFile.setFileName(emptyFileName);
            transaction.save(storeFile);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckSave = domainObjectSource.get(StoreFileReadable.class, objectId);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(emptyFileName, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave.getContentType());

        //Редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = domainObjectSource.get(StoreFileEditable.class, objectId);
            obj.setContentType(null);
            transaction.save(obj);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckEdit = domainObjectSource.get(StoreFileReadable.class, objectId);
        Assert.assertNotNull(storeFileCheckEdit);
        Assert.assertEquals(emptyFileName, storeFileCheckEdit.getFileName());
        Assert.assertNull(storeFileCheckEdit.getContentType());
    }

    @Test
    public void saveEmptyDomainObject() throws Exception {
        final long objectId = 1;
        final String emptyFileName = "";
        final String contentType = "info.json";

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
            storeFile.setContentType(contentType);
            storeFile.setFileName(emptyFileName);
            transaction.save(storeFile);
            transaction.save(storeFile);
        });

        //Загружаем сохраненый объект и сразу без редактирования полей вызываем сохранение
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.get(StoreFileEditable.class, objectId);
            transaction.save(obj);
        });
    }

    @Test
    public void removeOneObject() throws Exception {
        //Проверяем, что таких объектов нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 2L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 3L));

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            transaction.save(transaction.create(StoreFileEditable.class));
            transaction.save(transaction.create(StoreFileEditable.class));
            transaction.save(transaction.create(StoreFileEditable.class));
        });

        //Проверяем что файлы сохранены
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 3L));

        //Удяляем 2-й объект
        domainObjectSource.executeTransactional(transaction -> transaction.remove(transaction.get(StoreFileEditable.class, 2L)));

        //Проверяем, корректность удаления
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 3L));
    }

    @Test
    public void removeReferencedObject() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable file = transaction.create(StoreFileEditable.class);
            file.setFolderId(folder.getId());
            transaction.save(file);
        });

        try {
            domainObjectSource.executeTransactional(transaction -> transaction.remove(transaction.get(ExchangeFolderEditable.class, 1)));
            Assert.fail();
        } catch (ForeignDependencyException ex) {
            try (IteratorEntity<ExchangeFolderReadable> i = domainObjectSource.find(ExchangeFolderReadable.class, EmptyFilter.INSTANCE)) {
                Assert.assertTrue(i.hasNext());
            }
        }

        domainObjectSource.executeTransactional(transaction -> {
            transaction.remove(transaction.get(StoreFileEditable.class, 1));
            transaction.remove(transaction.get(ExchangeFolderEditable.class, 1));
        });
    }

    @Test
    public void removeAll() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);
        });

        domainObjectSource.executeTransactional(transaction -> transaction.removeAll(ExchangeFolderEditable.class));
        try (IteratorEntity<ExchangeFolderReadable> i = domainObjectSource.find(ExchangeFolderReadable.class, EmptyFilter.INSTANCE)) {
            Assert.assertFalse(i.hasNext());
        }
        new DomainService(domainObjectSource.getDbProvider(), schema)
                .setChangeMode(ChangeMode.NONE)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(ExchangeFolderEditable.class))
                .execute();

        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(folder.getId());
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(folder.getId());
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);
        });

        try {
            domainObjectSource.executeTransactional(transaction -> transaction.removeAll(ExchangeFolderEditable.class));
            Assert.fail();
        } catch (ForeignDependencyException e) {
            try (IteratorEntity<ExchangeFolderReadable> i = domainObjectSource.find(ExchangeFolderReadable.class, EmptyFilter.INSTANCE)) {
                Assert.assertTrue(i.hasNext());
            }
        }

        domainObjectSource.executeTransactional(transaction -> {
            transaction.removeAll(StoreFileEditable.class);
            transaction.removeAll(ExchangeFolderEditable.class);
        });
    }
}
