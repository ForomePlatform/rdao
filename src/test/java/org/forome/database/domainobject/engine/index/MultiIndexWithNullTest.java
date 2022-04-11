package org.forome.database.domainobject.engine.index;

import org.forome.database.Record;
import org.forome.database.RecordIterator;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class MultiIndexWithNullTest extends StoreFileDataTest {

    @Test
    public void findByComboIndex() throws Exception {
        final int recordCount = 100;

        recordSource.executeTransactional(transaction -> {
            for (long size = 0; size < recordCount; size++) {
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"name", "size"}, new Object[]{null, size});
            }
        });

        for (long size = 0; size < recordCount; size++) {
            try (RecordIterator i = recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new HashFilter(StoreFileReadable.FIELD_SIZE, size).appendField(StoreFileReadable.FIELD_FILE_NAME, null))){
                Assertions.assertThat(i.hasNext()).isTrue();

                Record record = i.next();
                Assertions.assertThat(record.getValues()[StoreFileReadable.FIELD_SIZE]).isEqualTo(size);
                Assertions.assertThat(record.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isEqualTo(null);
                Assertions.assertThat(i.hasNext()).isFalse();
            }
        }
    }
}
