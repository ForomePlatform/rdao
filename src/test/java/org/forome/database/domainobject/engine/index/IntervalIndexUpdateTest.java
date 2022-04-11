package org.forome.database.domainobject.engine.index;

import org.forome.domain.StoreFileReadable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class IntervalIndexUpdateTest extends IntervalTest {

    @Test
    public void removeRecords() throws Exception {
        prepareData();

        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        recordSource.executeTransactional(transaction -> transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 4L));
        assertValueEquals(Arrays.asList(-4L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        recordSource.executeTransactional(transaction -> transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 2L));
        assertValueEquals(Arrays.asList(-4L, 0L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);
    }

    @Test
    public void removeThenInsertRecords() throws Exception {
        prepareData();

        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        recordSource.executeTransactional(transaction -> transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 4L));
        assertValueEquals(Arrays.asList(-4L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        recordSource.executeTransactional(transaction -> transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{-2L}));

        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);
    }

    @Test
    public void updateRecords() throws Exception {
        prepareData();

        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        recordSource.executeTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 4L, new String[]{"size"}, new Object[]{3L}));
        assertValueEquals(Arrays.asList(-4L, 0L, 3L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        recordSource.executeTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 4L, new String[]{"size"}, new Object[]{8L}));
        assertValueEquals(Arrays.asList(-4L, 0L, 3L, 5L, 8L), StoreFileReadable.FIELD_SIZE, -5L, 10L);
    }


    private void prepareData() throws Exception {
        recordSource.executeTransactional(transaction -> {
            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{5L});
            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{3L});
            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{0L});
            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{-2L});
            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{-4L});
            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{-9L});
        });
    }
}
