package org.forome.database.domainobject.index;

import org.forome.database.domainobject.iterator.IntervalIndexIteratorTest;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.junit.Test;

import java.util.Arrays;

public class IntervalIndexUpdateTest extends IntervalIndexIteratorTest {

    @Test
    public void removeRecords() throws Exception {
        prepareData();

        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        domainObjectSource.executeTransactional(transaction -> transaction.remove(transaction.get(StoreFileEditable.class, 4)));
        assertValueEquals(Arrays.asList(-4L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        domainObjectSource.executeTransactional(transaction -> transaction.remove(transaction.get(StoreFileEditable.class, 2)));
        assertValueEquals(Arrays.asList(-4L, 0L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);
    }

    @Test
    public void removeThenInsertRecords() throws Exception {
        prepareData();

        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        domainObjectSource.executeTransactional(transaction -> transaction.remove(transaction.get(StoreFileEditable.class, 4)));
        assertValueEquals(Arrays.asList(-4L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-2);
            transaction.save(obj);
        });
        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);
    }

    @Test
    public void updateRecords() throws Exception {
        prepareData();

        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.get(StoreFileEditable.class, 4);
            obj.setSize(3);
            transaction.save(obj);
        });
        assertValueEquals(Arrays.asList(-4L, 0L, 3L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.get(StoreFileEditable.class, 4);
            obj.setSize(8);
            transaction.save(obj);
        });
        assertValueEquals(Arrays.asList(-4L, 0L, 3L, 5L, 8L), StoreFileReadable.FIELD_SIZE, -5L, 10L);
    }


    private void prepareData() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(3);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-4);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-9);
            transaction.save(obj);
        });
    }
}
