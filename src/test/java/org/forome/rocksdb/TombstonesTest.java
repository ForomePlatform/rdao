package org.forome.rocksdb;

import org.forome.database.domainobject.DomainDataTest;
import org.forome.database.exception.ColumnFamilyNotFoundException;
import org.forome.database.schema.Schema;
import org.forome.database.utils.TypeConvert;
import org.forome.domain.DataTestEditable;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

public class TombstonesTest extends DomainDataTest {

    @Test
    public void test() throws Exception {
        final int fillCount = 1_000_000;

        fillData(fillCount);
        checkForTombstones(fillCount);

        zebraStretchDelete((fillCount));
        checkForTombstones(fillCount);
    }

    private void checkForTombstones(int count) throws ColumnFamilyNotFoundException {
        String columnFamily = Schema.getEntity(DataTestEditable.class).getColumnFamily();
        ColumnFamilyHandle columnFamilyHandle = rocksDBProvider.getColumnFamilyHandle(columnFamily);
        try(ReadOptions readOptions = new ReadOptions().setMaxSkippableInternalKeys(1)) {
            for (long i = 1; i <= count; i++) {
                try (RocksIterator it = rocksDBProvider.getRocksDB().newIterator(columnFamilyHandle, readOptions)) {
                    it.seek(TypeConvert.pack(i));
                    Assert.assertTrue(it.isValid());
                }
            }
        }
    }

    private void fillData(int count) throws Exception {
        createDomain(DataTestEditable.class);
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 1; i <= count; ++i) {
                DataTestEditable dataTestEditable = transaction.create(DataTestEditable.class);
                dataTestEditable.setValue(String.valueOf(i));
                transaction.save(dataTestEditable);
            }
        });
    }

    private void zebraStretchDelete(int count) throws Exception {
        int stretchDeleteCount = 5;
        domainObjectSource.executeTransactional(transaction -> {
            for (long i = 1; i < count - (stretchDeleteCount + 1); i += (stretchDeleteCount + 1)) {
                for (long j = i; j < i + stretchDeleteCount; j++) {
                    DataTestEditable dataTestEditable = transaction.get(DataTestEditable.class, j);
                    transaction.remove(dataTestEditable);
                }
            }
        });
    }
}