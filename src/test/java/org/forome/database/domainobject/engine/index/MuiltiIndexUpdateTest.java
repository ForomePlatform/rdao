package org.forome.database.domainobject.engine.index;

import org.forome.database.Record;
import org.forome.database.RecordIterator;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Created by kris on 22.04.17.
 */
public class MuiltiIndexUpdateTest extends StoreFileDataTest {

    @Test
    public void run() throws Exception {
        //Добавляем объекты
        recordSource.executeTransactional(transaction -> {
            for (int i=1; i<=10; i++) {
                transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"name", "size"}, new Object[]{(i%2==0)?"2":"1", 100L});
            }
        });

        //Редактируем 1-й объект
        recordSource.executeTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1L, new String[]{"size"}, new Object[]{99L}));

        //Ищем объекты по size
        int count=0;
        try(RecordIterator i = recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new HashFilter(StoreFileReadable.FIELD_SIZE, 100L)
                .appendField(StoreFileReadable.FIELD_FILE_NAME, "1"))) {
            while(i.hasNext()) {
                Record storeFile = i.next();
                Assertions.assertThat(storeFile)
                        .isNotNull()
                        .matches(record -> record.getValues()[StoreFileReadable.FIELD_SIZE].equals(100L) &&
                                record.getValues()[StoreFileReadable.FIELD_FILE_NAME].equals("1"));
                count++;
            }
        }
        Assertions.assertThat(count).isEqualTo(4);
    }
}
