package org.forome.database.domainobject.index;

import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.database.domainobject.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class MuiltiIndexUpdateTest extends StoreFileDataTest {

    @Test
    public void run() throws Exception {
        //Добавляем объекты
        domainObjectSource.executeTransactional(transaction -> {
                for (int i=1; i<=10; i++) {
                    StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                    storeFile.setFileName((i%2==0)?"2":"1");
                    storeFile.setSize(100);
                    transaction.save(storeFile);
                }
        });

        //Редактируем 1-й объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
                storeFile.setSize(99);
                transaction.save(storeFile);
        });


        //Ищем объекты по size
        int count=0;
        try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, 100L)
             .appendField(StoreFileReadable.FIELD_FILE_NAME, "1"))) {
            while(iStoreFileReadable.hasNext()) {
                StoreFileReadable storeFile = iStoreFileReadable.next();

                Assert.assertNotNull(storeFile);
                Assert.assertEquals(100L, storeFile.getSize());
                Assert.assertEquals("1", storeFile.getFileName());

                count++;
            }

        }
        Assert.assertEquals(4, count);
    }
}
