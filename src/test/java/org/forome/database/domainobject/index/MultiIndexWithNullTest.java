package org.forome.database.domainobject.index;

import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.database.domainobject.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

public class MultiIndexWithNullTest extends StoreFileDataTest {

    @Test
    public void findByComboIndex() throws Exception {
        final int recordCount = 100;

        domainObjectSource.executeTransactional(transaction -> {
            for (long size = 0; size < recordCount; size++) {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setSize(size);
                storeFile.setFileName(null);
                transaction.save(storeFile);
            }
        });

        for (long size = 0; size < recordCount; size++) {
            try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class,
                    new HashFilter(StoreFileReadable.FIELD_SIZE, size).appendField(StoreFileReadable.FIELD_FILE_NAME, null))) {
                Assert.assertTrue(i.hasNext());

                StoreFileReadable storeFileReadable = i.next();

                Assert.assertEquals(size, storeFileReadable.getSize());
                Assert.assertNull(storeFileReadable.getFileName());

                Assert.assertFalse(i.hasNext());
            }
        }
    }
}
