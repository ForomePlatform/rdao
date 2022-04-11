package org.forome.rocksdb;

import org.forome.database.utils.TypeConvert;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.*;

public class OptimisticTransactionTest extends RocksDataTest {

    final byte[] key1= TypeConvert.pack("key1");
    final byte[] value1_old = TypeConvert.pack("value1_old");
    final byte[] value1_new = TypeConvert.pack("value1_new");

    final byte[] key2 = TypeConvert.pack("key2");
    final byte[] value2_old = TypeConvert.pack("value2_old");
    final byte[] value2_new = TypeConvert.pack("value2_new");

    @Test
    public void commit() throws RocksDBException {
        try(final Options options = new Options().setCreateIfMissing(true);
            final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString());
            final WriteOptions writeOptions = new WriteOptions();
            final ReadOptions readOptions = new ReadOptions()) {

            txnDb.put(writeOptions, key1, value1_old);

            try(final Transaction txn = txnDb.beginTransaction(writeOptions)) {
                txn.put(key1, value1_new);
                Assert.assertArrayEquals(value1_new, txn.get(readOptions, key1));
                txn.commit();
            }

            Assert.assertArrayEquals(value1_new, txnDb.get(readOptions, key1));
        }
    }

    @Test
    public void leaveTransaction() throws RocksDBException {
        try(final Options options = new Options().setCreateIfMissing(true);
            final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString());
            final WriteOptions writeOptions = new WriteOptions();
            final ReadOptions readOptions = new ReadOptions()) {

            txnDb.put(writeOptions, key1, value1_old);

            try(final Transaction txn = txnDb.beginTransaction(writeOptions)) {
                txn.put(key1, value1_new);
                txn.put(key2, value2_new);
                Assert.assertArrayEquals(value1_new, txn.get(readOptions, key1));
            }

            Assert.assertArrayEquals(value1_old, txnDb.get(readOptions, key1));
            Assert.assertNull(txnDb.get(readOptions, key2));
        }
    }

    @Test
    public void rollback() throws RocksDBException {
        try(final Options options = new Options().setCreateIfMissing(true);
            final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString());
            final WriteOptions writeOptions = new WriteOptions();
            final ReadOptions readOptions = new ReadOptions()) {

            txnDb.put(writeOptions, key1, value1_old);

            try(final Transaction txn = txnDb.beginTransaction(writeOptions)) {
                txn.put(key1, value1_new);
                Assert.assertArrayEquals(value1_new, txn.get(readOptions, key1));
                txn.rollback();
            }

            Assert.assertArrayEquals(value1_old, txnDb.get(readOptions, key1));
        }
    }

    @Test
    public void readOutside() throws RocksDBException {
        try(final Options options = new Options().setCreateIfMissing(true);
            final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString())) {

            try(final WriteOptions writeOptions = new WriteOptions();
                final Transaction txn = txnDb.beginTransaction(writeOptions);
                final ReadOptions readOptions = new ReadOptions()) {

                byte[] value = txn.get(readOptions, key1);
                Assert.assertNull(value);

                // Write a key in this transaction
                txn.put(key1, value1_old);

                // Read a key OUTSIDE this transaction. Does not affect txn.
                value = txnDb.get(readOptions, key1);
                Assert.assertNull(value);

                // Write a key OUTSIDE of this transaction.
                // Does not affect txn since this is an unrelated key.
                // If we wrote key 'abc' here, the transaction would fail to commit.
                txnDb.put(writeOptions, key2, value2_old);

                txn.commit();
            }
        }
    }

    @Test
    public void writeInsideThanOutside() throws RocksDBException {
        try(final Options options = new Options().setCreateIfMissing(true);
            final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString())) {

            try(final WriteOptions writeOptions = new WriteOptions();
                final Transaction txn = txnDb.beginTransaction(writeOptions)) {

                txn.put(key1, value1_old);

                txnDb.put(writeOptions, key1, value1_new);

                try {
                    txn.commit();
                } catch (RocksDBException e) {
                    Assert.assertTrue(true);
                    return;
                }
            }
        }
        Assert.fail();
    }

    @Test
    public void writeInsideThanWriteAnotherInside() throws RocksDBException {
        try(final Options options = new Options().setCreateIfMissing(true);
            final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString());
            final WriteOptions writeOptions = new WriteOptions()) {

            // Put initial value#2
            txnDb.put(writeOptions, key2, value2_old);

            try(final Transaction txn1 = txnDb.beginTransaction(writeOptions);
                final ReadOptions readOptions = new ReadOptions()) {
                // Put value#2 inside transacion#1
                txn1.put(key1, value1_old);

                // Read initial value#2 inside transacion#1
                try (final RocksIterator iter = txn1.getIterator(readOptions)) {
                    iter.seek(key2);
                    Assert.assertTrue(iter.isValid());
                    Assert.assertArrayEquals(key2, iter.key());
                    Assert.assertArrayEquals(value2_old, iter.value());
                }

                // Change value#2 inside transaction#2
                try(final Transaction txn2 = txnDb.beginTransaction(writeOptions)) {
                    txn2.put(key2, value2_new);
                    txn2.commit();
                }

                // Read new value#2 inside transacion#1
                try (final RocksIterator iter = txn1.getIterator(readOptions)) {
                    iter.seek(key2);
                    Assert.assertTrue(iter.isValid());
                    Assert.assertArrayEquals(key2, iter.key());
                    Assert.assertArrayEquals(value2_new, iter.value());
                }

                txn1.commit();
            }
        }
    }

    @Test
    public void reopenDB() throws Exception {
        final int reopenCount = 20;

        try (final Options options = new Options().setCreateIfMissing(true);
             final OptimisticTransactionDB db = OptimisticTransactionDB.open(options, pathDataBase.toString())) {

            db.put(key1, value1_old);
        }

        for (int i = 0; i < reopenCount; ++i) {
            try (final Options options = new Options().setCreateIfMissing(true);
                 final OptimisticTransactionDB db = OptimisticTransactionDB.open(options, pathDataBase.toString())) {

                try (RocksIterator iter = db.newIterator())
                {}

                value1_old[0] = (byte) i;
                db.put(key1, value1_old);
            }
        }

        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
    }
}
