package org.forome.database.domainobject.engine.index;

import org.forome.database.Record;
import org.forome.database.RecordIterator;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class IndexUpdateTest extends StoreFileDataTest {

    @Test
    public void update() throws Exception {
        final long oldValue = 100;

        //Добавляем объект
        long id = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{oldValue}));

        //Редактируем объект
        recordSource.executeTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, id, new String[]{"size"}, new Object[]{99L}));

        //Ищем объекты по size
        try (RecordIterator i = recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new HashFilter(StoreFileReadable.FIELD_SIZE, oldValue))){
            Assertions.assertThat(i.hasNext()).isFalse();
        }
    }

    @Test
    public void partialUpdate1() throws Exception {
        final long prevValue = 100;

        //Добавляем объекты
        recordSource.executeTransactional(transaction -> {
            for (int i= 1; i <= 2; i++) {
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{prevValue});
            }
        });

        //Редактируем 1-й объект
        recordSource.executeTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1, new String[]{"size"}, new Object[]{99L}));

        //Ищем объекты по size
        try (RecordIterator i = recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new HashFilter(StoreFileReadable.FIELD_SIZE, prevValue))){
            Record record = i.next();
            Assertions.assertThat(record.getId()).isEqualTo(2L);
            Assertions.assertThat(record.getValues()[StoreFileReadable.FIELD_SIZE]).isEqualTo(prevValue);
        }
    }

    @Test
    public void partialUpdate2() throws Exception {
        final long value = 100;

        //Добавляем объекты
        recordSource.executeTransactional(transaction -> {
            for (int i = 1; i <= 10; i++) {
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"size"}, new Object[]{value});
            }
        });

        //Редактируем 1-й объект
        recordSource.executeTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1, new String[]{"size"}, new Object[]{99L}));

        //Ищем объекты по size
        try (RecordIterator i = recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new HashFilter(StoreFileReadable.FIELD_SIZE, value))){
            int count = 0;
            while (i.hasNext()) {
                Record record = i.next();
                Assertions.assertThat(record.getId()).isNotEqualTo(1L);
                Assertions.assertThat(record.getValues()[StoreFileReadable.FIELD_SIZE]).isEqualTo(value);

                count++;
            }
            Assertions.assertThat(count).isEqualTo(9);
        }
    }
}
