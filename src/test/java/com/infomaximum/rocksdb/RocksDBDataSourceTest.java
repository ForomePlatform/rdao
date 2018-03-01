package com.infomaximum.rocksdb;

import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.utils.TypeConvert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RocksDBDataSourceTest extends RocksDataTest {

    private RocksDBProvider rocksDBProvider;

    private static final String columnFamily = "test_cf";
    private static final long startValue = 0x8000000080000000L;
    private static final int valueCount = 100;

    @Before
    public void init() throws Exception {
        super.init();

        rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build();
    }

    @After
    public void destroy() throws Exception {
        rocksDBProvider.close();

        super.destroy();
    }

    @Test
    public void seekWithStrictMatching() throws Exception {
        fillData();

        try (DBIterator iterator = rocksDBProvider.createIterator(columnFamily)) {
            KeyValue keyValue = iterator.seek(new KeyPattern(TypeConvert.pack(0x6000000080000000L), true));
            Assert.assertNull(keyValue);

            byte[] pattern = TypeConvert.pack(0x6000000080000001L);
            keyValue = iterator.seek(new KeyPattern(pattern, true));
            Assert.assertArrayEquals(pattern, keyValue.getKey());

            keyValue = iterator.seek(new KeyPattern(TypeConvert.pack(0x7000000080000000L), false));
            Assert.assertArrayEquals(TypeConvert.pack(0x7000000080000001L), keyValue.getKey());

            pattern = TypeConvert.pack(0x6000000080000001L);
            keyValue = iterator.seek(new KeyPattern(pattern, false));
            Assert.assertArrayEquals(pattern, keyValue.getKey());
        }
    }

    @Test
    public void step() throws Exception {
        fillData();

        // test iterate by previous
        try (DBIterator iterator = rocksDBProvider.createIterator(columnFamily)) {
            iterator.seek(new KeyPattern(TypeConvert.pack(0xA0000000)));
            for (long i = startValue + valueCount - 1; i >= startValue; --i) {
                KeyValue keyValue = iterator.step(DBIterator.StepDirection.BACKWARD);
                Assert.assertEquals(i, TypeConvert.unpackLong(keyValue.getKey()).longValue());
            }
        }

        // test iterate by next
        try (DBIterator iterator = rocksDBProvider.createIterator(columnFamily)) {
            iterator.seek(new KeyPattern(TypeConvert.pack(0x70000000)));
            for (long i = startValue; i < startValue + valueCount; ++i) {
                KeyValue keyValue = iterator.step(DBIterator.StepDirection.FORWARD);
                Assert.assertEquals(i, TypeConvert.unpackLong(keyValue.getKey()).longValue());
            }
        }

        try (DBIterator iterator = rocksDBProvider.createIterator(columnFamily)) {
            final KeyValue firstKeyValue = iterator.seek(null);
            Assert.assertArrayEquals(TypeConvert.pack(0x6000000080000001L), firstKeyValue.getKey());

            KeyValue keyValue = iterator.step(DBIterator.StepDirection.BACKWARD);
            Assert.assertNull(keyValue);

            final KeyValue lastKeyValue = iterator.seek(new KeyPattern(TypeConvert.pack(0xA0000000)));
            Assert.assertArrayEquals(TypeConvert.pack(0xA000000080000001L), lastKeyValue.getKey());

            keyValue = iterator.step(DBIterator.StepDirection.FORWARD);
            Assert.assertNull(keyValue);
        }
    }

    private void fillData() throws Exception {
        rocksDBProvider.createColumnFamily(columnFamily);

        try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
            transaction.put(columnFamily, TypeConvert.pack(0x6000000080000001L), TypeConvert.EMPTY_BYTE_ARRAY);
            transaction.put(columnFamily, TypeConvert.pack(0x7000000080000001L), TypeConvert.EMPTY_BYTE_ARRAY);
            transaction.put(columnFamily, TypeConvert.pack(0xA000000080000001L), TypeConvert.EMPTY_BYTE_ARRAY);

            for (int i = 0; i < valueCount; i++) {
                transaction.put(columnFamily, TypeConvert.pack(startValue + i), TypeConvert.EMPTY_BYTE_ARRAY);
            }

            transaction.commit();
        }
    }
}
