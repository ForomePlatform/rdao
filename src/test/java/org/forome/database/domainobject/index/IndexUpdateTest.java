package org.forome.database.domainobject.index;

import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.database.domainobject.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

public class IndexUpdateTest extends StoreFileDataTest {

    @Test
    public void update() throws Exception {
        final long oldValue = 100;

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
            storeFile.setSize(oldValue);
            transaction.save(storeFile);
        });

        //Редактируем объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
            storeFile.setSize(99);
            transaction.save(storeFile);
        });

        //Ищем объекты по size
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, oldValue))) {
            Assert.assertFalse(i.hasNext());
        }
    }

    @Test
    public void partialUpdate1() throws Exception {
        final long prevValue = 100;

        //Добавляем объекты
        domainObjectSource.executeTransactional(transaction -> {
            for (int i= 1; i <= 2; i++) {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setSize(prevValue);
                transaction.save(storeFile);
            }
        });

        //Редактируем 1-й объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
            storeFile.setSize(99);
            transaction.save(storeFile);
        });

        //Ищем объекты по size
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, prevValue))) {
            StoreFileReadable storeFile = i.next();
            Assert.assertEquals(2L, storeFile.getId());
            Assert.assertEquals(prevValue, storeFile.getSize());

            Assert.assertFalse(i.hasNext());
        }
    }

    @Test
    public void partialUpdate2() throws Exception {
        final long value = 100;

        //Добавляем объекты
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 1; i <= 10; i++) {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setSize(value);
                transaction.save(storeFile);
            }
        });

        //Редактируем 1-й объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
            storeFile.setSize(99);
            transaction.save(storeFile);
        });

        //Ищем объекты по size
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, value))) {
            int count = 0;
            while(i.hasNext()) {
                StoreFileReadable storeFile = i.next();

                Assert.assertTrue(storeFile.getId() != 1);
                Assert.assertEquals(value, storeFile.getSize());

                count++;
            }
            Assert.assertEquals(9, count);
        }
    }
}
