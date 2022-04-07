package org.forome.rocksdb;

import org.forome.database.provider.DBTransaction;
import org.forome.database.utils.TypeConvert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDBDropColumnTest extends RocksDataTest {

    @Test
    public void dropColumnFamily() throws Exception {
        String cfName = "org.infomaximum.store.StoreFile.index";

        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            rocksDBProvider.createColumnFamily(cfName);

            try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
                transaction.put(cfName, "key".getBytes(), "value".getBytes());
                transaction.commit();
            }
        }

        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            rocksDBProvider.dropColumnFamily(cfName);
        }

        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
    }

    @Test
    public void dropColumnFamilyNative() throws Exception {
        final byte[] cfName = "test".getBytes();

        // create DB with new column family
        try(final Options options = new Options().setCreateIfMissing(true);
            final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString());
            final WriteOptions writeOptions = new WriteOptions()) {

            try (ColumnFamilyHandle cf = txnDb.createColumnFamily(new ColumnFamilyDescriptor(cfName))) {
                txnDb.put(cf, writeOptions, "key1".getBytes(), "value1".getBytes());
                txnDb.put(cf, writeOptions, "key2".getBytes(), "value2".getBytes());

                try (final Transaction txn = txnDb.beginTransaction(writeOptions)) {
                    txn.put(cf, "key3".getBytes(), "value3".getBytes());
                    txn.commit();
                }
            }
        }

        // drop column family
        List<ColumnFamilyDescriptor> desc = new ArrayList<>();
        try(DBOptions options = new DBOptions();
            Options anotherOptions = new Options()) {
            int cfIndex = -1;
            for (byte[] columnFamilyName : RocksDB.listColumnFamilies(anotherOptions, pathDataBase.toString())) {
                desc.add(new ColumnFamilyDescriptor(columnFamilyName));
            }
            if (desc.isEmpty()) {
                desc.add(new ColumnFamilyDescriptor(TypeConvert.pack(RocksDBProvider.DEFAULT_COLUMN_FAMILY)));
            }
            for (int i = 0; i < desc.size(); ++i) {
                if(Arrays.equals(desc.get(i).getName(), cfName)) {
                    cfIndex = i;
                    break;
                }
            }

            List<ColumnFamilyHandle> cfs = new ArrayList<>();
            try (OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString(), desc, cfs)) {
                txnDb.dropColumnFamily(cfs.get(cfIndex));
                cfs.forEach(AbstractImmutableNativeReference::close);
            }
        }

        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
    }
}
