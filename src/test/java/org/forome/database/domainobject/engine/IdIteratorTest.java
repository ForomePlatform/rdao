package org.forome.database.domainobject.engine;

import org.forome.database.Record;
import org.forome.database.RecordIterator;
import org.forome.database.domainobject.DomainObjectSource;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.filter.EmptyFilter;
import org.forome.database.domainobject.filter.IdFilter;
import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.exception.DatabaseException;
import org.forome.domain.StoreFileEditable;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class IdIteratorTest extends StoreFileDataTest {

    @Test
    public void filter() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        assertFilter(1, 10, new IdFilter(0, Long.MAX_VALUE));
        assertFilter(5, 7, new IdFilter(5, 7));
        assertFilter(0, 0, new IdFilter(50, 70));
        assertFilter(1, 10, new IdFilter(1, 11));
        assertFilter(1, 1, new IdFilter(1, 1));

        domainObjectSource.executeTransactional(transaction -> {
            try (IteratorEntity<StoreFileEditable> i = domainObjectSource.find(StoreFileEditable.class, EmptyFilter.INSTANCE)) {
                while (i.hasNext()) {
                    transaction.remove(i.next());
                }
            }
        });
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        assertFilter(0, 0L, new IdFilter(0, 10));
        assertFilter(11, 20L, new IdFilter(5, Long.MAX_VALUE));
        assertFilter(11, 15L, new IdFilter(5, 15));
        assertFilter(11, 11L, new IdFilter(11, 11));
    }

    @Test
    public void loadTwoFields() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator i = recordSource.select("StoreFile", "org.infomaximum.store", new IdFilter(0))) {
            int iteratedRecordCount = 0;
            while (i.hasNext()) {
                Record storeFile = i.next();
                ++iteratedRecordCount;
            }
            Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
        }
    }

    private void assertFilter(final long expectedFromId, final long expectedToId, IdFilter filter) throws DatabaseException {
        long expectedRecordCount = expectedToId == expectedFromId && expectedToId == 0
                ? 0
                : (expectedToId - expectedFromId) + 1;

        try (RecordIterator i = recordSource.select("StoreFile", "org.infomaximum.store", filter)) {
            long iteratedRecordCount = 0;
            long currId = expectedFromId;
            Record storeFile = null;
            while (i.hasNext()) {
                storeFile = i.next();

                Assert.assertEquals(currId, storeFile.getId());
                ++iteratedRecordCount;
                ++currId;
            }
            if (storeFile != null) {
                Assert.assertEquals(expectedToId, storeFile.getId());
            }
            Assert.assertEquals(expectedRecordCount, iteratedRecordCount);
        }
    }

    @Test
    public void removeAndFind() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        recordSource.executeTransactional(transaction -> {
            transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1L);
            transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 5L);

            assertFind(transaction, new IdFilter(0, 1));
            assertFind(transaction, new IdFilter(0, 1000), 2,3,4,6,7,8,9,10);
        });

        assertFind(new IdFilter(0, 1));
        assertFind(new IdFilter(0, 1000), 2,3,4,6,7,8,9,10);
    }

    private void initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(10);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
                transaction.save(obj);
            }
        });
    }
}
