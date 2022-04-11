package org.forome.rocksdb;

import org.forome.database.utils.TypeConvert;
import org.junit.Test;
import org.rocksdb.*;
import org.rocksdb.Transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NativeTransactionFailsAtConflictTest extends RocksDataTest {

    @Test
    public void test() throws Exception {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections.singletonList(new ColumnFamilyDescriptor(TypeConvert.pack(RocksDBProvider.DEFAULT_COLUMN_FAMILY)));
        int fillCount = 1_500_000;

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        try (DBOptions options = new DBOptions().setCreateIfMissing(true);
             OptimisticTransactionDB db = OptimisticTransactionDB.open(options, pathDataBase.toString(), columnFamilyDescriptors, columnFamilyHandles)) {
            try (WriteOptions writeOptions = new WriteOptions();
                 Transaction transaction = db.beginTransaction(writeOptions)) {

                for (int i = 0; i < fillCount; i++) {
                    transaction.put(("key:" + i).getBytes(), ("value:" + i).getBytes());
                }
                transaction.commit();
            }

            columnFamilyHandles.forEach(ColumnFamilyHandle::close);
            columnFamilyHandles.clear();
            try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)){
                db.flush(flushOptions);
            }
        }

        try(DBOptions options = new DBOptions();
            OptimisticTransactionDB db = OptimisticTransactionDB.open(options, pathDataBase.toString(), columnFamilyDescriptors, columnFamilyHandles)) {
            try (WriteOptions writeOptions = new WriteOptions();
                 Transaction transaction = db.beginTransaction(writeOptions)) {

                transaction.delete(("key:" + 1).getBytes());
                transaction.commit();
            }

            columnFamilyHandles.forEach(ColumnFamilyHandle::close);
            columnFamilyHandles.clear();
        }
    }
}
