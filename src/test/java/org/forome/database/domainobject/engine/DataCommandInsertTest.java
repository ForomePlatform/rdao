package org.forome.database.domainobject.engine;

import org.forome.database.Record;
import org.forome.database.RecordIterator;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.filter.*;
import org.infomaximum.database.domainobject.filter.*;
import org.forome.database.exception.ForeignDependencyException;
import org.forome.database.utils.TableUtils;
import org.forome.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DataCommandInsertTest extends StoreFileDataTest {

    @Test
    public void insertSimple() throws Exception {
        String[] fields = new String[]{"name"};
        Object[] values = new Object[]{"objName"};
        long id = recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord("StoreFile", "org.infomaximum.store", fields, values));
        assertThatDBContainsRecord(id, fields, values, "StoreFile", "org.infomaximum.store");
    }

    @Test
    public void insertWithTwoValueFields() throws Exception {
        String[] fields = new String[]{"size", "end_time"};
        Object[] values = new Object[]{3L, Instant.now()};
        long id = recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord("StoreFile", "org.infomaximum.store", fields, values));
        assertThatDBContainsRecord(id, fields, values, "StoreFile", "org.infomaximum.store");
    }

    @Test
    public void insertWithAllFields() throws Exception {
        String[] fields = new String[]{"size",
                "name",
                "data",
                "single",
                "double",
                "begin_time",
                "end_time",
                "begin",
                "end",
                "local_begin",
                "local_end"};
        Object[] values = new Object[]{3L,
                "name",
                "bytes".getBytes(),
                false,
                123.34,
                Instant.now(),
                Instant.now(),
                12L,
                14L,
                LocalDateTime.now(),
                LocalDateTime.now(),
        };
        long id = recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord("StoreFile", "org.infomaximum.store", fields, values));
        assertThatDBContainsRecord(id, fields, values, "StoreFile", "org.infomaximum.store");
    }

    @Test
    public void insertWithAllAndDependencyFields() throws Exception {
        String tableName = "StoreFile";
        String namespace = "org.infomaximum.store";
        String[] folderFields = new String[] {"uuid"};
        String[] folderFieldValues = new String[] {"uuid"};
        long folderId = recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord("ExchangeFolder", "org.infomaximum.exchange", folderFields, folderFieldValues));

        LocalDateTime localBegin = LocalDateTime.ofEpochSecond(1604589065, 0, ZoneOffset.UTC);
        LocalDateTime localEnd = LocalDateTime.ofEpochSecond(1604599965, 0, ZoneOffset.UTC);
        Instant beginTime = Instant.ofEpochSecond(1604589065);
        Instant endTime = Instant.ofEpochSecond(1604599965);
        String[] fields = new String[]{"size",
                "name",
                "type",
                "data",
                "single",
                "folder_id",
                "double",
                "begin_time",
                "end_time",
                "begin",
                "end",
                "local_begin",
                "local_end"};
        Object[] values = new Object[]{3L,
                "name",
                "nameType",
                "bytes".getBytes(),
                false,
                folderId,
                123.34,
                beginTime,
                endTime,
                12L,
                14L,
                localBegin,
                localEnd,
        };
        long id = recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord(tableName, namespace, fields, values));
        assertThatDBContainsRecord(id, fields, values, tableName, namespace);
        Record expected = buildRecord(id, fields, values, tableName, namespace);
        assertThatFilteredRecordsContainsExactly(Collections.singletonList(expected), tableName, namespace,
                new HashFilter(StoreFileReadable.FIELD_SIZE, 3L),
                new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name"),
                new HashFilter(StoreFileReadable.FIELD_SIZE, 3L).appendField(StoreFileReadable.FIELD_FILE_NAME, "name"),
                new HashFilter(StoreFileReadable.FIELD_LOCAL_BEGIN, localBegin),
                new HashFilter(StoreFileReadable.FIELD_LOCAL_BEGIN, localBegin),

                new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "na"),
                new PrefixFilter(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_CONTENT_TYPE), "nam"),

                new IntervalFilter(StoreFileReadable.FIELD_SIZE, 2L, 4L),
                new IntervalFilter(StoreFileReadable.FIELD_DOUBLE, 120d, 124d),
                new IntervalFilter(StoreFileReadable.FIELD_BEGIN_TIME, beginTime.minusMillis(10000), endTime),
                new IntervalFilter(StoreFileReadable.FIELD_LOCAL_BEGIN, localBegin.minusMinutes(1), localEnd),
                new IntervalFilter(StoreFileReadable.FIELD_SIZE, 2L, 4L).appendHashedField(StoreFileReadable.FIELD_FILE_NAME, "name"),
                new IntervalFilter(StoreFileReadable.FIELD_SIZE, 2L, 4L).appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, folderId),

                new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN, StoreFileReadable.FIELD_END), 11L, 13L),
                new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN, StoreFileReadable.FIELD_END), 11L, 13L).appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, folderId),
                new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN_TIME, StoreFileReadable.FIELD_END_TIME), beginTime.minusMillis(10000), beginTime.plusMillis(1)).appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, folderId)
        );
    }

    @Test
    public void insertManyObjectsWithAllAndDependencyFields() throws Exception {
        String tableName = "StoreFile";
        String namespace = "org.infomaximum.store";
        String[] folderFields = new String[] {"uuid"};
        String[] folderFieldValues = new String[] {"uuid"};
        long folderId = recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord("ExchangeFolder", "org.infomaximum.exchange", folderFields, folderFieldValues));

        LocalDateTime localBegin = LocalDateTime.ofEpochSecond(1604589065, 0, ZoneOffset.UTC);
        LocalDateTime localEnd = LocalDateTime.ofEpochSecond(1604599965, 0, ZoneOffset.UTC);
        Instant beginTime = Instant.ofEpochSecond(1604589065);
        Instant endTime = Instant.ofEpochSecond(1604599965);
        String[] fields = new String[]{"size",
                "name",
                "type",
                "data",
                "single",
                "folder_id",
                "double",
                "begin_time",
                "end_time",
                "begin",
                "end",
                "local_begin",
                "local_end"};
        Object[] values = new Object[]{3L,
                "name",
                "nameType",
                "bytes".getBytes(),
                false,
                folderId,
                123.34,
                beginTime,
                endTime,
                12L,
                14L,
                localBegin,
                localEnd,
        };
        long id = recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord(tableName, namespace, fields, values));
        insertStoreFilesData(tableName, namespace);
        assertThatDBContainsRecord(id, fields, values, tableName, namespace);
        Record expected = buildRecord(id, fields, values, tableName, namespace);
        assertThatFilteredRecordsContainsExactly(Collections.singletonList(expected), tableName, namespace,
                new HashFilter(StoreFileReadable.FIELD_SIZE, 3L),
                new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name"),
                new HashFilter(StoreFileReadable.FIELD_SIZE, 3L).appendField(StoreFileReadable.FIELD_FILE_NAME, "name"),
                new HashFilter(StoreFileReadable.FIELD_LOCAL_BEGIN, localBegin),
                new HashFilter(StoreFileReadable.FIELD_LOCAL_BEGIN, localBegin),

                new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "na"),
                new PrefixFilter(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_CONTENT_TYPE), "nam"),

                new IntervalFilter(StoreFileReadable.FIELD_SIZE, 2L, 4L),
                new IntervalFilter(StoreFileReadable.FIELD_DOUBLE, 120d, 124d),
                new IntervalFilter(StoreFileReadable.FIELD_BEGIN_TIME, beginTime.minusMillis(10000), endTime),
                new IntervalFilter(StoreFileReadable.FIELD_LOCAL_BEGIN, localBegin.minusMinutes(1), localEnd),
                new IntervalFilter(StoreFileReadable.FIELD_SIZE, 2L, 4L).appendHashedField(StoreFileReadable.FIELD_FILE_NAME, "name"),
                new IntervalFilter(StoreFileReadable.FIELD_SIZE, 2L, 4L).appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, folderId),

                new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN, StoreFileReadable.FIELD_END), 11L, 13L),
                new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN, StoreFileReadable.FIELD_END), 11L, 13L).appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, folderId),
                new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN_TIME, StoreFileReadable.FIELD_END_TIME), beginTime.minusMillis(10000), beginTime.plusMillis(1)).appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, folderId)
        );
    }

    @Test
    public void failForeignDependencyExceptionNotExistingReferencingObject() {
        String tableName = "StoreFile";
        String namespace = "org.infomaximum.store";

        String[] fields = new String[]{"size",
                "folder_id"};
        Object[] values = new Object[]{3L,
                3L
        };
        Assertions.assertThatThrownBy(() -> recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord(tableName, namespace, fields, values)))
                .isInstanceOf(ForeignDependencyException.class);
    }

    private void insertStoreFilesData(String tableName, String namespace) throws Exception {
        String[] folderFields = new String[] {"uuid"};
        String[] folderFieldValues = new String[] {"uuid"};
        long folderId = recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord("ExchangeFolder", "org.infomaximum.exchange", folderFields, folderFieldValues));

        String[] fields = new String[]{"size",
                "name",
                "type",
                "data",
                "single",
                "folder_id",
                "double",
                "begin_time",
                "end_time",
                "begin",
                "end",
                "local_begin",
                "local_end"};
        Object[] values = new Object[]{5L,
                "name2",
                "nameType2",
                "bytes2".getBytes(),
                true,
                folderId,
                13.34,
                Instant.now(),
                Instant.now(),
                110L,
                111L,
                LocalDateTime.now(),
                LocalDateTime.now(),
        };
        recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord(tableName, namespace, fields, values));

        String[] fields2 = new String[]{"size",
                "name",
                "type",
                "data",
                "single",
                "folder_id",
                "double",
                "begin_time",
                "end_time",
                "begin",
                "end",
                "local_begin",
                "local_end"};
        Object[] values2 = new Object[]{6L,
                "name3",
                "nameType3",
                "bytes3".getBytes(),
                true,
                folderId,
                1311.34,
                Instant.now(),
                Instant.now(),
                10L,
                11L,
                LocalDateTime.now(),
                LocalDateTime.now(),
        };
        recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord(tableName, namespace, fields2, values2));

        String[] fields3 = new String[]{"size",
                "name",
                "type",
                "data",
                "single",
                "folder_id",
                "double",
                "begin_time",
                "end_time",
                "begin",
                "end",
                "local_begin",
                "local_end"};
        Object[] values3 = new Object[]{7L,
                "name4",
                "nameType3",
                "bytes3".getBytes(),
                true,
                folderId,
                11.34,
                Instant.now(),
                Instant.now(),
                25L,
                45L,
                LocalDateTime.now(),
                LocalDateTime.now(),
        };
        recordSource.executeFunctionTransactional(dataCommand ->
                dataCommand.insertRecord(tableName, namespace, fields3, values3));

    }

    private Record buildRecord(long id, String[] fields, Object[] values, String table, String namespace) {
        Object[] sortedValues = TableUtils.sortValuesByFieldOrder(table, namespace, fields, values, schema.getDbSchema());
        return new Record(id, sortedValues);
    }

    private void assertThatFilteredRecordsContainsExactly(List<Record> expected, String table, String namespace, Filter... filters) {
        for (Filter filter : filters) {
            assertIndexFilter(expected, filter, table, namespace);
        }
    }


    private void assertThatDBContainsRecord(long id, String[] fields, Object[] values, String table, String namespace) {
        try (RecordIterator i = recordSource.select(table, namespace)) {
            while (i.hasNext()) {
                Record record = i.next();
                if (record.getId() == id) {
                    Object[] actualValues = TableUtils.sortValuesByFieldOrder(table, namespace, fields, values, schema.getDbSchema());
                    Assertions.assertThat(record.getValues()).isEqualTo(actualValues);
                    return;
                }
            }
        }
        Assertions.fail("БД не содержит заданный record");
    }

    private void assertIndexFilter(List<Record> expected, Filter filter, String table, String namespace) {
        List<Record> actual = new ArrayList<>();
        try (RecordIterator i = selectIterator(table, namespace, filter)){
            while (i.hasNext()) {
                actual.add(i.next());
            }
        }
        Assertions.assertThat(actual).as("TableName: %s, Namespace: %s, Filter: %s", table, namespace, filter.getClass().getName()).containsAll(expected);
    }

    private RecordIterator selectIterator(String table, String namespace, Filter filter) {
        if (filter instanceof HashFilter) {
            return recordSource.select(table, namespace, (HashFilter) filter);
        }
        if (filter instanceof PrefixFilter) {
            return recordSource.select(table, namespace, (PrefixFilter) filter);
        }
        if (filter instanceof IntervalFilter) {
            return recordSource.select(table, namespace, (IntervalFilter) filter);
        }
        if (filter instanceof RangeFilter) {
            return recordSource.select(table, namespace, (RangeFilter) filter);
        }
        throw new UnsupportedOperationException(Filter.class.getName());
    }
}