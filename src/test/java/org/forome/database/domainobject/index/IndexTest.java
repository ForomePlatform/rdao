package org.forome.database.domainobject.index;

import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.database.exception.IndexNotFoundException;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.domain.type.FormatType;
import org.forome.database.domainobject.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

public class IndexTest extends StoreFileDataTest {

    @Test
    public void notFoundIndex() {
        try {
            domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_BEGIN_TIME, null));
            Assert.fail();
        } catch (IndexNotFoundException ignore) {}

        try {
            domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_BEGIN_TIME, null).appendField(StoreFileReadable.FIELD_SIZE, null));
            Assert.fail();
        } catch (IndexNotFoundException ignore) {}
    }

    @Test
    public void findByIndex() throws Exception {
        final int recordCount = 100;

        domainObjectSource.executeTransactional(transaction -> {
            for (long size = 0; size < recordCount; size++) {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setSize(size);
                transaction.save(storeFile);
            }
        });

        for (long size = 0; size < recordCount; size++) {
            try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, size))) {
                Assert.assertTrue(i.hasNext());
                Assert.assertEquals(size, i.next().getSize());
                Assert.assertFalse(i.hasNext());
            }
        }
    }

    @Test
    public void findByEnumIndex() throws Exception {
        final int recordCount = 100;

        domainObjectSource.executeTransactional(transaction -> {
            for (long size = 0; size < recordCount; size++) {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setFormat(FormatType.A);
                transaction.save(storeFile);

                storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setFormat(FormatType.B);
                transaction.save(storeFile);

                storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setFormat(null);
                transaction.save(storeFile);
            }
        });

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FORMAT, FormatType.A))) {
            long id = 1;
            int count = 0;
            while (i.hasNext()) {
                StoreFileReadable obj = i.next();
                Assert.assertEquals(id, obj.getId());
                Assert.assertEquals(FormatType.A, obj.getFormat());
                id += 3;
                ++count;
            }
            Assert.assertEquals(recordCount, count);
        }

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FORMAT, null))) {
            long id = 3;
            int count = 0;
            while (i.hasNext()) {
                StoreFileReadable obj = i.next();
                Assert.assertEquals(id, obj.getId());
                Assert.assertNull(obj.getFormat());
                id += 3;
                ++count;
            }
            Assert.assertEquals(recordCount, count);
        }
    }

    @Test
    public void findByPartialUpdatedMultiIndex() throws Exception {
        final int recordCount = 100;
        final String fileName = "file_name";

        // insert new objects
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setSize(i);
                storeFile.setFileName(fileName);
                transaction.save(storeFile);
            }
        });

        // update part of multi-indexed object
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable storeFile = transaction.get(StoreFileEditable.class, i + 1);
                storeFile.setSize(i + 2 * recordCount);
                transaction.save(storeFile);
            }
        });

        // find
        for (long size = (2 * recordCount); size < (recordCount + 2 * recordCount); size++) {
            try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, size))) {
                Assert.assertTrue(i.hasNext());
                Assert.assertEquals(size, i.next().getSize());
                Assert.assertFalse(i.hasNext());
            }

            HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, size).appendField(StoreFileReadable.FIELD_FILE_NAME, fileName);
            try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, filter)) {
                Assert.assertTrue(i.hasNext());
                Assert.assertEquals(size, i.next().getSize());
                Assert.assertFalse(i.hasNext());
            }
        }
    }
}
