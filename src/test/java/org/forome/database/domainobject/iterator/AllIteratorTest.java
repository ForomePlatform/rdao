package org.forome.database.domainobject.iterator;

import org.forome.database.provider.DBTransaction;
import org.forome.database.domainobject.DomainObjectSource;
import org.forome.database.domainobject.Transaction;
import org.forome.database.domainobject.filter.EmptyFilter;
import org.forome.database.exception.UnexpectedEndObjectException;
import org.forome.database.schema.Schema;
import org.forome.database.utils.TypeConvert;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.domain.type.FormatType;
import org.forome.database.domainobject.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class AllIteratorTest extends StoreFileDataTest {

    @Test
    public void checkInnerStructure() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
            transaction.delete(Schema.getEntity(StoreFileReadable.class).getColumnFamily(), TypeConvert.pack(2L));
            transaction.commit();
        }

        try (IteratorEntity iterator = domainObjectSource.find(StoreFileReadable.class, EmptyFilter.INSTANCE, Collections.singleton(StoreFileReadable.FIELD_SIZE))) {
            while (iterator.hasNext()) {
                iterator.next();
            }
            Assert.fail();
        } catch (UnexpectedEndObjectException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void orderingIterate() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
            int iteratedRecordCount = 0;
            long prevId = 0;
            while (iStoreFileReadable.hasNext()) {
                StoreFileReadable storeFile = iStoreFileReadable.next();

                if (prevId == storeFile.getId()) Assert.fail("Fail next object");
                if (prevId >= storeFile.getId()) Assert.fail("Fail sort id to iterators");
                prevId = storeFile.getId();
                ++iteratedRecordCount;
            }
            Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
        }
    }

    @Test
    public void loadTwoFields() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        Set<Integer> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, EmptyFilter.INSTANCE, loadingFields)) {
            int iteratedRecordCount = 0;
            while (i.hasNext()) {
                StoreFileReadable storeFile = i.next();

                checkLoadedState(storeFile, loadingFields);

                ++iteratedRecordCount;
            }
            Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
        }
    }

    @Test
    public void loadZeroFields() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        Set<Integer> loadingFields = Collections.emptySet();
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, EmptyFilter.INSTANCE, loadingFields)) {
            int iteratedRecordCount = 0;
            while (i.hasNext()) {
                StoreFileReadable storeFile = i.next();

                checkLoadedState(storeFile, loadingFields);

                ++iteratedRecordCount;
            }

            Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
        }
    }

    @Test
    public void iterateTransactional() {
        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            // insert
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(10);
            obj.setFormat(FormatType.B);
            transaction.save(obj);

            Assert.assertEquals(10L, transaction.get(StoreFileReadable.class, obj.getId()).getSize());

            // change
            obj.setSize(20);
            transaction.save(obj);

            try (IteratorEntity<StoreFileReadable> i = transaction.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
                Assert.assertEquals(20L, i.next().getSize());
            }

            // change
            obj.setFormat(null);
            transaction.save(obj);

            StoreFileReadable storedObj = transaction.get(StoreFileReadable.class, obj.getId());
            Assert.assertNull(storedObj.getFormat());

            transaction.commit();
        }
    }

    @Test
    public void iterateAndChange() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            try (IteratorEntity<StoreFileReadable> i = transaction.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
                while (i.hasNext()) {
                    StoreFileReadable current = i.next();
                    if (current.getId() == insertedRecordCount) {
                        break;
                    }

                    StoreFileEditable newNext = transaction.get(StoreFileEditable.class, current.getId() + 1);
                    newNext.setDouble(Double.POSITIVE_INFINITY);
                    newNext.setFileName(UUID.randomUUID().toString());
                    transaction.save(newNext);

                    StoreFileReadable next = i.next();
                    Assert.assertEquals("name", next.getFileName());
                    Assert.assertNull(next.getDouble());
                }
            }
        }
    }

    @Test
    public void removeAndFind() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        domainObjectSource.executeTransactional(transaction -> {
            transaction.remove(transaction.get(StoreFileEditable.class, 1));
            transaction.remove(transaction.get(StoreFileEditable.class, 5));

            Assert.assertNull(transaction.get(StoreFileReadable.class, 1));
            assertFind(transaction, EmptyFilter.INSTANCE, 2,3,4,6,7,8,9,10);
        });

        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1));
        assertFind(domainObjectSource, EmptyFilter.INSTANCE, 2,3,4,6,7,8,9,10);
    }

    private void initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(10);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
                obj.setFormat(FormatType.B);
                transaction.save(obj);
            }
        });
    }
}
