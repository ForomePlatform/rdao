package org.forome.database.domainobject.engine.index;

import org.forome.database.Record;
import org.forome.database.RecordIterator;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.database.exception.IndexNotFoundException;
import org.forome.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class IndexTest extends StoreFileDataTest {

    @Test
    public void notFoundIndex() {
        Assertions.assertThatThrownBy(() -> recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new HashFilter(StoreFileReadable.FIELD_BEGIN_TIME, null))).isInstanceOf(IndexNotFoundException.class);

        Assertions.assertThatThrownBy(() -> recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new HashFilter(StoreFileReadable.FIELD_BEGIN_TIME, null).appendField(StoreFileReadable.FIELD_SIZE, null)))
                .isInstanceOf(IndexNotFoundException.class);
    }

    @Test
    public void findByIndex() throws Exception {
        final int recordCount = 100;

        recordSource.executeTransactional(transaction -> {
            for (long size = 0; size < recordCount; size++) {
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{size});
            }
        });

        for (long size = 0; size < recordCount; size++) {
            try (RecordIterator i = recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new HashFilter(StoreFileReadable.FIELD_SIZE, size))){
                Assertions.assertThat(i.hasNext()).isTrue();
                Assertions.assertThat(i.next().getValues()[StoreFileReadable.FIELD_SIZE]).isEqualTo(size);
                Assertions.assertThat(i.hasNext()).isFalse();
            }
        }
    }

    @Test
    public void findByPartialUpdatedMultiIndex() throws Exception {
        final int recordCount = 100;
        final String fileName = "file_name";

        // insert new objects
        recordSource.executeTransactional(transaction -> {
            for (long size = 0; size < recordCount; size++) {
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size", "name"}, new Object[]{size, fileName});
            }
        });

        // update part of multi-indexed object
        recordSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                Record storeFile = transaction.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, i+1);
                storeFile.getValues()[StoreFileReadable.FIELD_SIZE] = i + 2 * recordCount;
            }
        });

        // find
        for (long size = 1; size < recordCount; size++) {
            try (RecordIterator i = recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new HashFilter(StoreFileReadable.FIELD_SIZE, size))) {
                Assertions.assertThat(i.hasNext()).isTrue();
                Assertions.assertThat(i.next().getValues()[StoreFileReadable.FIELD_SIZE]).isEqualTo(size);
                Assertions.assertThat(i.hasNext()).isFalse();
            }

            HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, size).appendField(StoreFileReadable.FIELD_FILE_NAME, fileName);
            try (RecordIterator i = recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, filter)) {
                Assertions.assertThat(i.hasNext()).isTrue();
                Assertions.assertThat(i.next().getValues()[StoreFileReadable.FIELD_SIZE]).isEqualTo(size);
                Assertions.assertThat(i.hasNext()).isFalse();
            }
        }
    }
}
