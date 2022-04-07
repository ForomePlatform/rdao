package org.forome.database.domainobject.index;

import org.forome.database.domainobject.filter.PrefixFilter;
import org.forome.database.exception.IndexNotFoundException;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.KeyPattern;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.Schema;
import org.forome.database.schema.StructEntity;
import org.forome.database.utils.key.Key;
import org.forome.database.utils.key.PrefixIndexKey;
import org.forome.database.utils.PrefixIndexUtils;
import org.forome.database.utils.TypeConvert;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.database.domainobject.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PrefixIndexTest extends StoreFileDataTest {

    private final String fileName = " Test sTring info@mail.com \r";
    private final List<String> lexemes = Arrays.asList("test", "string", "info@mail.com", "mail.com", "com");

    private String indexColumnFamily;

    @Before
    public void init() throws Exception {
        super.init();

        indexColumnFamily = Schema.getEntity(StoreFileReadable.class).getPrefixIndexes().get(0).columnFamily;
    }

    @Test
    public void notFoundIndex() {
        try {
            domainObjectSource.find(StoreFileReadable.class, new PrefixFilter(StoreFileReadable.FIELD_BEGIN_TIME, null));
            Assert.fail();
        } catch (IndexNotFoundException ignore) {}

        try {
            domainObjectSource.find(StoreFileReadable.class, new PrefixFilter(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE), null), null);
            Assert.fail();
        } catch (IndexNotFoundException ignore) {}
    }

    @Test
    public void insertWithNotOverflowedBlock() throws Exception {
        final int recordCount = 10;
        ByteBuffer buffer = createRecords(recordCount);

        List<String> currentLexemes = new ArrayList<>(lexemes);
        try (DBIterator iterator = rocksDBProvider.createIterator(indexColumnFamily)) {
            KeyValue keyValue = iterator.seek(buildAttendantPrefixIndex());
            while (keyValue != null) {
                assertEquals(0, buffer.array(), currentLexemes, keyValue);

                keyValue = iterator.next();
            }
        }

        Assert.assertEquals(0, currentLexemes.size());
    }

    @Test
    public void insertWithOverflowedBlock() throws Exception {
        final int recordCountForFullBlock = PrefixIndexUtils.PREFERRED_MAX_ID_COUNT_PER_BLOCK;
        ByteBuffer bufferForFullBlock = createRecords(recordCountForFullBlock);

        final int recordCount = 10;
        ByteBuffer buffer = createRecords(recordCount);

        List<String> currentLexemes = new ArrayList<>(lexemes);
        try (DBIterator iterator = rocksDBProvider.createIterator(indexColumnFamily)) {
            KeyValue keyValue = iterator.seek(buildAttendantPrefixIndex());
            while (keyValue != null) {
                PrefixIndexKey key = PrefixIndexKey.unpack(keyValue.getKey());
                Assert.assertEquals(0, key.getBlockNumber());
                Assert.assertArrayEquals(bufferForFullBlock.array(), keyValue.getValue());

                keyValue = iterator.next();
                assertEquals(1, buffer.array(), currentLexemes, keyValue);

                keyValue = iterator.next();
            }
        }

        Assert.assertEquals(0, currentLexemes.size());
    }

    @Test
    public void reinsertWithOverflowedBlock() throws Exception {
        final int recordCountForFullBlock = PrefixIndexUtils.PREFERRED_MAX_ID_COUNT_PER_BLOCK;
        ByteBuffer bufferForFullBlock = createRecords(recordCountForFullBlock);

        final int recordCount = 10;
        ByteBuffer buffer = createRecords(recordCount);

        final long updatingId1 = 7;
        final long updatingId2 = PrefixIndexUtils.PREFERRED_MAX_ID_COUNT_PER_BLOCK + 2;
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.get(StoreFileEditable.class, updatingId1);
            obj.setFileName(null);
            transaction.save(obj);

            obj = transaction.get(StoreFileEditable.class, updatingId2);
            obj.setFileName(null);
            transaction.save(obj);
        });

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.get(StoreFileEditable.class, updatingId1);
            obj.setFileName(fileName);
            transaction.save(obj);

            obj = transaction.get(StoreFileEditable.class, updatingId2);
            obj.setFileName(fileName);
            transaction.save(obj);
        });

        List<String> currentLexemes = new ArrayList<>(lexemes);
        try (DBIterator iterator = rocksDBProvider.createIterator(indexColumnFamily)) {
            KeyValue keyValue = iterator.seek(buildAttendantPrefixIndex());
            while (keyValue != null) {
                PrefixIndexKey key = PrefixIndexKey.unpack(keyValue.getKey());
                Assert.assertEquals(0, key.getBlockNumber());
                Assert.assertArrayEquals(bufferForFullBlock.array(), keyValue.getValue());

                keyValue = iterator.next();
                assertEquals(1, buffer.array(), currentLexemes, keyValue);

                keyValue = iterator.next();
            }
        }

        Assert.assertEquals(0, currentLexemes.size());
    }

    @Test
    public void remove() throws Exception {
        final int recordCount = 10;
        byte[] buffer = createRecords(recordCount).array();

        final long removingId = 7;
        domainObjectSource.executeTransactional(transaction -> transaction.remove(transaction.get(StoreFileEditable.class, removingId)));
        buffer = PrefixIndexUtils.removeId(removingId, buffer);

        List<String> currentLexemes = new ArrayList<>(lexemes);
        try (DBIterator iterator = rocksDBProvider.createIterator(indexColumnFamily)) {
            KeyValue keyValue = iterator.seek(buildAttendantPrefixIndex());
            while (keyValue != null) {
                assertEquals(0, buffer, currentLexemes, keyValue);

                keyValue = iterator.next();
            }
        }

        Assert.assertEquals(0, currentLexemes.size());
    }

    @Test
    public void update() throws Exception {
        final int recordCount = 1;
        ByteBuffer buffer = createRecords(recordCount);

        final String newFileName = " Test sTrIng inform@mail. \r";
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.get(StoreFileEditable.class, 1);
            obj.setFileName(newFileName);
            transaction.save(obj);
        });

        List<String> currentLexemes = new ArrayList<>(Arrays.asList("test", "string", "inform@mail.", "mail."));
        try (DBIterator iterator = rocksDBProvider.createIterator(indexColumnFamily)) {
            KeyValue keyValue = iterator.seek(buildAttendantPrefixIndex());
            while (keyValue != null) {
                assertEquals(0, buffer.array(), currentLexemes, keyValue);

                keyValue = iterator.next();
            }
        }

        Assert.assertEquals(0, currentLexemes.size());
    }

    private static void assertEquals(int expectedBlock, byte[] expectedValue, List<String> expectedLexemes, KeyValue actual) {
        PrefixIndexKey key = PrefixIndexKey.unpack(actual.getKey());
        Assert.assertTrue(expectedLexemes.remove(key.getLexeme()));
        Assert.assertEquals(expectedBlock, key.getBlockNumber());
        Assert.assertArrayEquals(expectedValue, actual.getValue());
    }

    private ByteBuffer createRecords(int recordCount) throws Exception {
        ByteBuffer idsBuffer = TypeConvert.allocateBuffer(recordCount * Key.ID_BYTE_SIZE);
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName(fileName);
                transaction.save(obj);
                idsBuffer.putLong(obj.getId());
            }
        });

        return idsBuffer;
    }

    private KeyPattern buildAttendantPrefixIndex() {
        StructEntity structEntity = Schema.getEntity(StoreFileEditable.class);
        return PrefixIndexKey.buildKeyPatternForFind("", structEntity.getPrefixIndex(Collections.singleton(0)));
    }
}
