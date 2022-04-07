package org.forome.database.domainobject.engine;

import org.forome.database.Record;
import org.forome.database.RecordIterator;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.Transaction;
import org.forome.database.domainobject.filter.IntervalFilter;
import org.forome.database.domainobject.filter.SortDirection;
import org.forome.database.exception.DatabaseException;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IntervalIndexIteratorTest extends StoreFileDataTest {

    @Test
    public void longOrderTest() throws Exception {
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

        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);
        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -4L, 5L);
        assertValueEquals(Arrays.asList(-4L, -2L), StoreFileReadable.FIELD_SIZE, -4L, -1L);
        assertValueEquals(Arrays.asList(-2L, 0L, 3L), StoreFileReadable.FIELD_SIZE, -3L, 4L);
        assertValueEquals(Collections.singletonList(3L), StoreFileReadable.FIELD_SIZE, 1L, 4L);
        assertValueEquals(Collections.emptyList(), StoreFileReadable.FIELD_SIZE, 6L, 8L);
    }

    @Test
    public void dateOrderTest() throws Exception {
        final long currentTime = System.currentTimeMillis();

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setBeginTime(Instant.ofEpochMilli(currentTime));
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setBeginTime(Instant.ofEpochMilli(currentTime + 1000));
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setBeginTime(Instant.ofEpochMilli(currentTime + 5 * 1000));
            transaction.save(obj);
        });

        assertValueEquals(Arrays.asList(Instant.ofEpochMilli(currentTime), Instant.ofEpochMilli(currentTime + 1000)), StoreFileReadable.FIELD_BEGIN_TIME, Instant.ofEpochMilli(currentTime), Instant.ofEpochMilli(currentTime + 1000));
        assertValueEquals(Arrays.asList(Instant.ofEpochMilli(currentTime + 1000), Instant.ofEpochMilli(currentTime + 5 * 1000)), StoreFileReadable.FIELD_BEGIN_TIME, Instant.ofEpochMilli(currentTime + 500), Instant.ofEpochMilli(currentTime + 6 * 1000));
    }

    @Test
    public void localDateOrderTest() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setLocalBegin(LocalDateTime.of(2018, 10, 22, 18, 32));
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setLocalBegin(LocalDateTime.of(2018, 10, 22, 18, 33));
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setLocalBegin(LocalDateTime.of(2018, 10, 22, 18, 34));
            transaction.save(obj);
        });

        assertValueEquals(Arrays.asList(
                LocalDateTime.of(2018, 10, 22, 18, 32),
                LocalDateTime.of(2018, 10, 22, 18, 33),
                LocalDateTime.of(2018, 10, 22, 18, 34)
        ), new IntervalFilter(
                StoreFileReadable.FIELD_LOCAL_BEGIN,
                LocalDateTime.of(2018, 10, 22, 18, 30),
                LocalDateTime.of(2018, 10, 22, 18, 35)
        ));
    }

    @Test
    public void doubleOrderTest() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(5.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(3.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(2.0001);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(2.0000089);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(-0.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(0.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(-2.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(-4.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(-9.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.MIN_VALUE);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.MAX_VALUE);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.NEGATIVE_INFINITY);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.POSITIVE_INFINITY);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.NaN);
            transaction.save(obj);
        });

        assertValueEquals(Arrays.asList(-4.0, -2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0, 5.0), StoreFileReadable.FIELD_DOUBLE, -5.0, 10.0);
        assertValueEquals(Arrays.asList(-4.0, -2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0, 5.0), StoreFileReadable.FIELD_DOUBLE, -4.0, 5.0);
        assertValueEquals(Arrays.asList(-4.0, -2.0), StoreFileReadable.FIELD_DOUBLE, -4.0, -1.0);
        assertValueEquals(Arrays.asList(-2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0), StoreFileReadable.FIELD_DOUBLE, -3.0, 4.0);
        assertValueEquals(Arrays.asList(2.0000089, 2.0001, 3.0), StoreFileReadable.FIELD_DOUBLE, 2.0, 4.0);
        assertValueEquals(Collections.emptyList(), StoreFileReadable.FIELD_DOUBLE, 6.0, 8.0);

        assertValueEquals(Collections.singletonList(Double.MAX_VALUE), StoreFileReadable.FIELD_DOUBLE, 1000.0, Double.MAX_VALUE);
        assertValueEquals(Arrays.asList(Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN), StoreFileReadable.FIELD_DOUBLE, Double.MAX_VALUE, Double.NaN);
        assertValueEquals(Arrays.asList(Double.NEGATIVE_INFINITY, -9.0, -4.0, -2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0, 5.0, Double.MAX_VALUE, Double.POSITIVE_INFINITY),
                StoreFileReadable.FIELD_DOUBLE,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @Test
    public void stringGroupedOrderTest() throws Exception {
        final String name1 = "name1";
        final String name2 = "name2";

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            obj.setFileName(name1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(3);
            obj.setFileName(name1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(0);
            obj.setFileName(name2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-2);
            obj.setFileName(name2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-4);
            obj.setFileName(name1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-9);
            obj.setFileName(name2);
            transaction.save(obj);
        });

        assertValueEquals(Arrays.asList(-4L, 3L, 5L), new IntervalFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L)
                .appendHashedField(StoreFileReadable.FIELD_FILE_NAME, name1));
        assertValueEquals(Collections.singletonList(-2L), new IntervalFilter(StoreFileReadable.FIELD_SIZE, -5L, -1L)
                .appendHashedField(StoreFileReadable.FIELD_FILE_NAME, name2));
        assertValueEquals(Collections.emptyList(), new IntervalFilter(StoreFileReadable.FIELD_SIZE, -5L, -1L)
                .appendHashedField(StoreFileReadable.FIELD_FILE_NAME, "name3"));
    }

    @Test
    public void longGroupedOrderTest() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            transaction.setForeignFieldEnabled(false);

            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            obj.setFolderId(1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(3);
            obj.setFolderId(1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(3);
            obj.setFolderId(2);
            transaction.save(obj);
        });

        assertIdEquals(Arrays.asList(2L, 1L), new IntervalFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L)
                .appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, 1L));
        assertIdEquals(Collections.singletonList(1L), new IntervalFilter(StoreFileReadable.FIELD_SIZE, 5L, 5L)
                .appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, 1L));
        assertIdEquals(Collections.singletonList(3L), new IntervalFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L)
                .appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, 2L));
        assertIdEquals(Collections.emptyList(), new IntervalFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L)
                .appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, 3L));
    }

    @Test
    public void duplicateTest() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            transaction.save(obj);
        });

        assertIdEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L),
                new IntervalFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L));

        assertIdEquals(Arrays.asList(1L, 2L, 3L),
                new IntervalFilter(StoreFileReadable.FIELD_SIZE, -5L, 3L));

        assertIdEquals(Arrays.asList(2L, 3L, 4L, 5L, 6L),
                new IntervalFilter(StoreFileReadable.FIELD_SIZE, 2L, 5L));
    }

    @Test
    public void iterateAndChange() throws Exception {
        final String name = "name";

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName(name);
            obj.setSize(10);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName(name);
            obj.setSize(20);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName(name);
            obj.setSize(40);
            transaction.save(obj);
        });

        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            try (RecordIterator iterator = recordSource
                    .select("StoreFile", "org.infomaximum.store",
                            new IntervalFilter(StoreFileReadable.FIELD_SIZE, 10L, 20L)
                                    .appendHashedField(StoreFileEditable.FIELD_FILE_NAME, name))) {
                List<Long> ids = new ArrayList<>();
                while (iterator.hasNext()) {
                    StoreFileEditable s = transaction.get(StoreFileEditable.class, 3);
                    s.setSize(15);
                    transaction.save(s);

                    Record item = iterator.next();
                    ids.add(item.getId());
                }

                Assert.assertEquals(Arrays.asList(1L, 2L), ids);
            }
        }
    }

//    @Test
//    public void removeAndFind() throws Exception {
//        domainObjectSource.executeTransactional(transaction -> {
//            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(1);
//            transaction.save(obj);
//
//            obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(20);
//            transaction.save(obj);
//
//            obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(1);
//            transaction.save(obj);
//        });
//
//        domainObjectSource.executeTransactional(transaction -> {
//            transaction.remove(transaction.get(StoreFileEditable.class, 1));
//            transaction.remove(transaction.get(StoreFileEditable.class, 2));
//
//            testFind(transaction, new IntervalFilter(StoreFileReadable.FIELD_SIZE, 19L, 21L));
//            testFind(transaction, new IntervalFilter(StoreFileReadable.FIELD_SIZE, 0L, 1L), 3);
//        });
//
//        testFind(domainObjectSource, new IntervalFilter(StoreFileReadable.FIELD_SIZE, 19L, 21L));
//        testFind(domainObjectSource, new IntervalFilter(StoreFileReadable.FIELD_SIZE, 0L, 1L), 3);
//    }

    protected void assertValueEquals(List<Double> expected, Integer fieldName, Double begin, Double end) throws DatabaseException {
        assertValueEquals(expected, new IntervalFilter(fieldName, begin, end));
    }

    protected void assertValueEquals(List<Long> expected, Integer fieldName, Long begin, Long end) throws DatabaseException {
        assertValueEquals(expected, new IntervalFilter(fieldName, begin, end));
    }

    protected void assertValueEquals(List<Instant> expected, Integer fieldName, Instant begin, Instant end) throws DatabaseException {
        assertValueEquals(expected, new IntervalFilter(fieldName, begin, end));
    }

    private <T> void assertValueEquals(List<T> expected, IntervalFilter filter) throws DatabaseException {
        filter.setSortDirection(SortDirection.ASC);
        List<T> actual = getValues(filter);
        Assert.assertEquals(expected, actual);

        filter.setSortDirection(SortDirection.DESC);
        actual = getValues(filter);
        Collections.reverse(expected);
        Assert.assertEquals(expected, actual);
    }

    private void assertIdEquals(List<Long> expected, IntervalFilter filter) throws DatabaseException {
        filter.setSortDirection(SortDirection.ASC);
        List<Long> actual = getIds(filter);
        Assert.assertEquals(expected, actual);

        filter.setSortDirection(SortDirection.DESC);
        actual = getIds(filter);
        Collections.reverse(expected);
        Assert.assertEquals(expected, actual);
    }

    private <T> List<T> getValues(IntervalFilter filter) throws DatabaseException {
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store",  filter)) {
            List<T> result = new ArrayList<>();
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();

                result.add((T) storeFile.getValues()[filter.getIndexedFieldId()]);
            }
            return result;
        }
    }

    private List<Long> getIds(IntervalFilter filter) throws DatabaseException {
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "org.infomaximum.store", filter)) {
            List<Long> result = new ArrayList<>();
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();

                result.add(storeFile.getId());
            }
            return result;
        }
    }
}
