package org.forome.database.domainobject.iterator;

import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.DomainObjectSource;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.Transaction;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.database.utils.HashIndexUtils;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.domain.type.FormatType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.*;

public class HashIndexIteratorTest extends StoreFileDataTest {

    @Test
    public void ignoreCaseFind() throws Exception {

        final long sizeExpected = 10;
        final String nameExpected = "привет всем";

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет всем");
            obj.setSize(sizeExpected);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет");
            obj.setSize(sizeExpected);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("ПРИВЕТ ВСЕМ");
            obj.setSize(sizeExpected);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("всем");
            obj.setSize(sizeExpected);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("прИВет всЕм");
            obj.setSize(sizeExpected);
            transaction.save(obj);
        });

        Set<Integer> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
        HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, sizeExpected).appendField(StoreFileReadable.FIELD_FILE_NAME, nameExpected);
        try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.find(StoreFileReadable.class, filter, loadingFields)) {
            int iteratedRecordCount = 0;
            while (iterator.hasNext()) {
                StoreFileReadable storeFile = iterator.next();

                Assert.assertTrue(storeFile.getFileName().equalsIgnoreCase(nameExpected));
                ++iteratedRecordCount;
            }
            Assert.assertEquals(3, iteratedRecordCount);
        }
    }

    @Test
    public void loadTwoFields() throws Exception {
        initAndFillStoreFiles(domainObjectSource, 100);

        Set<Integer> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
        HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, 9L);
        try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.find(StoreFileReadable.class, filter, loadingFields)) {
            int iteratedRecordCount = 0;
            while (iterator.hasNext()) {
                StoreFileReadable storeFile = iterator.next();

                checkLoadedState(storeFile, loadingFields);

                ++iteratedRecordCount;
            }
            Assert.assertEquals(10, iteratedRecordCount);
        }
    }

    @Test
    public void loadNullableTwoFields() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < 10; i++) {
                transaction.save(transaction.create(StoreFileEditable.class));
            }
        });

        Field fieldValuesField = DomainObject.class.getDeclaredField("loadedFieldValues");
        fieldValuesField.setAccessible(true);

        Set<Integer> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
        HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, null);
        try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.find(StoreFileReadable.class, filter, loadingFields)) {
            int iteratedRecordCount = 0;
            while (iterator.hasNext()) {
                StoreFileReadable storeFile = iterator.next();
                Assert.assertNull(storeFile.get(StoreFileReadable.FIELD_SIZE));

                checkLoadedState(storeFile, loadingFields);

                ++iteratedRecordCount;
            }
            Assert.assertEquals(10, iteratedRecordCount);
        }
    }

    @Test
    public void loadZeroFields() throws Exception {
        initAndFillStoreFiles(domainObjectSource, 100);

        Set<Integer> loadingFields = Collections.emptySet();
        HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, 9L);
        try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.find(StoreFileReadable.class, filter, loadingFields)) {
            int iteratedRecordCount = 0;
            while (iterator.hasNext()) {
                StoreFileReadable storeFile = iterator.next();

                checkLoadedState(storeFile, loadingFields);

                ++iteratedRecordCount;
            }

            Assert.assertEquals(10, iteratedRecordCount);
        }
    }

    @Test
    public void findTransactional() {
        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            // insert
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(10);
            transaction.save(obj);

            Assert.assertEquals(10L, transaction.get(StoreFileReadable.class, obj.getId()).getSize());

            // change
            obj.setSize(20);
            transaction.save(obj);

            HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, 20L);
            try (IteratorEntity<StoreFileReadable> i = transaction.find(StoreFileReadable.class, filter)) {
                Assert.assertTrue(i.hasNext());
            }

            transaction.commit();
        }
    }

    @Test
    public void findBySingleField() throws Exception {

        final String nameExpected = "привет всем";

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет всем");
            obj.setSize(20);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет");
            obj.setSize(30);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("ПРИВЕТ ВСЕМ");
            obj.setSize(40);
            transaction.save(obj);
        });

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, 50L))) {
            Assert.assertFalse(i.hasNext());
        }

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, 30L))) {
            int count = 0;
            while (i.hasNext()) {
                i.next();
                ++count;
            }
            Assert.assertEquals(1, count);
        }

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "unknown"))) {
            Assert.assertFalse(i.hasNext());
        }

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, nameExpected), Collections.emptySet())) {
            Assert.assertTrue(i.hasNext());
        }

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, nameExpected))) {
            int count = 0;
            while (i.hasNext()) {
                i.next();
                ++count;
            }
            Assert.assertEquals(2, count);
        }
    }

    @Test
    public void testCollision() throws Exception {
        final int storedCount = 5;
        final String nameCol1 = "http://ccwf/login.aspx?login=auto&crid=20560116";
        final String nameCol2 = "http://ccwf/login.aspx?login=auto&crid=20517268";
        Assert.assertEquals("Hash of names must be equals",
                HashIndexUtils.buildHash(String.class, nameCol2, null), HashIndexUtils.buildHash(String.class, nameCol1, null));

        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < storedCount; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName(nameCol1);
                transaction.save(obj);
            }
        });

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, nameCol2))) {
            Assert.assertFalse(i.hasNext());
        }

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, nameCol1))) {
            int count = 0;
            while (i.hasNext()) {
                i.next();
                ++count;
            }
            Assert.assertEquals(storedCount, count);
        }
    }

    @Test
    public void iterateAndChange() throws Exception {
        final long value = 20;

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет всем");
            obj.setSize(value);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет");
            obj.setSize(value);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("ПРИВЕТ ВСЕМ");
            obj.setSize(40);
            obj.setLocalBegin(LocalDateTime.of(2018, 10, 22, 18, 32));
            transaction.save(obj);
        });

        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            try (IteratorEntity<StoreFileReadable> i = transaction.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, value))) {
                List<Long> ids = new ArrayList<>();
                while (i.hasNext()) {
                    StoreFileEditable s = transaction.get(StoreFileEditable.class, 3);
                    s.setSize(value);
                    transaction.save(s);

                    StoreFileReadable item = i.next();
                    Assert.assertEquals(value, item.getSize());

                    ids.add(item.getId());
                }

                Assert.assertEquals(Arrays.asList(1L, 2L), ids);
            }

            Assert.assertNotNull(transaction.find(
                    StoreFileReadable.class,
                    new HashFilter(StoreFileReadable.FIELD_LOCAL_BEGIN, LocalDateTime.of(2018, 10, 22, 18, 32))
            ));
        }
    }

    @Test
    public void removeAndFind() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(20);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(1);
            transaction.save(obj);
        });

        domainObjectSource.executeTransactional(transaction -> {
            transaction.remove(transaction.get(StoreFileEditable.class, 1));
            transaction.remove(transaction.get(StoreFileEditable.class, 2));

            assertFind(transaction, new HashFilter(StoreFileReadable.FIELD_SIZE, 20L));
            assertFind(transaction, new HashFilter(StoreFileReadable.FIELD_SIZE, 1L), 3);
        });

        assertFind(domainObjectSource, new HashFilter(StoreFileReadable.FIELD_SIZE, 20L));
        assertFind(domainObjectSource, new HashFilter(StoreFileReadable.FIELD_SIZE, 1L), 3);

        StoreFileEditable[] newObj = {null};
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(1);
            transaction.save(obj);

            try (IteratorEntity<StoreFileEditable> i = transaction.find(StoreFileEditable.class, new HashFilter(StoreFileEditable.FIELD_SIZE, 1L))) {
                transaction.remove(i.next());
            }

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(2);
            transaction.save(obj);
            newObj[0] = obj;
        });

        assertFind(domainObjectSource, new HashFilter(StoreFileReadable.FIELD_SIZE, 2L), newObj[0].getId());
    }

    private void initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(i % 10);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
                obj.setFormat(FormatType.B);
                transaction.save(obj);
            }
        });
    }
}
