package org.forome.database.domainobject.engine.index;

import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.filter.PrefixFilter;
import org.forome.database.exception.IndexNotFoundException;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.KeyPattern;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.Schema;
import org.forome.database.schema.StructEntity;
import org.forome.database.utils.PrefixIndexUtils;
import org.forome.database.utils.TypeConvert;
import org.forome.database.utils.key.Key;
import org.forome.database.utils.key.PrefixIndexKey;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PrefixIndexTest extends StoreFileDataTest {

    private final String fileName = " Test sTring info@mail.com \r";
    private final List<String> lexemes = Arrays.asList("test", "string", "info@mail.com", "mail.com", "com");

    private String indexColumnFamily;

    @BeforeEach
    public void init() throws Exception {
        super.init();
        indexColumnFamily = Schema.getEntity(StoreFileReadable.class).getPrefixIndexes().get(0).columnFamily;
    }

    @Test
    public void notFoundIndex() {
        Assertions.assertThatThrownBy(() -> recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new PrefixFilter(StoreFileReadable.FIELD_BEGIN_TIME, null)))
                .isInstanceOf(IndexNotFoundException.class);

        Assertions.assertThatThrownBy(() -> recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new PrefixFilter(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE), null)))
                .isInstanceOf(IndexNotFoundException.class);
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

        Assertions.assertThat(currentLexemes.size()).isEqualTo(0);
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

        Assertions.assertThat(currentLexemes.size()).isEqualTo(0);
    }

    @Test
    public void reinsertWithOverflowedBlock() throws Exception {
        final int recordCountForFullBlock = PrefixIndexUtils.PREFERRED_MAX_ID_COUNT_PER_BLOCK;
        ByteBuffer bufferForFullBlock = createRecords(recordCountForFullBlock);

        final int recordCount = 10;
        ByteBuffer buffer = createRecords(recordCount);

        final long updatingId1 = 7;
        final long updatingId2 = PrefixIndexUtils.PREFERRED_MAX_ID_COUNT_PER_BLOCK + 2;
        recordSource.executeTransactional(transaction -> {
            transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, updatingId1, new String[]{"name"}, new Object[]{null});
            transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, updatingId2, new String[]{"name"}, new Object[]{null});
        });
        recordSource.executeTransactional(transaction -> {
            transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, updatingId1, new String[]{"name"}, new Object[]{fileName});
            transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, updatingId2, new String[]{"name"}, new Object[]{fileName});
        });

        List<String> currentLexemes = new ArrayList<>(lexemes);
        try (DBIterator iterator = rocksDBProvider.createIterator(indexColumnFamily)) {
            KeyValue keyValue = iterator.seek(buildAttendantPrefixIndex());
            while (keyValue != null) {
                PrefixIndexKey key = PrefixIndexKey.unpack(keyValue.getKey());
                Assertions.assertThat(key.getBlockNumber()).isEqualTo(0);
                Assertions.assertThat(bufferForFullBlock.array()).containsExactly(keyValue.getValue());

                keyValue = iterator.next();
                assertEquals(1, buffer.array(), currentLexemes, keyValue);

                keyValue = iterator.next();
            }
        }

        Assertions.assertThat(currentLexemes.size()).isEqualTo(0);
    }

    @Test
    public void remove() throws Exception {
        final int recordCount = 10;
        byte[] buffer = createRecords(recordCount).array();

        final long removingId = 7;
        recordSource.executeTransactional(transaction -> transaction.deleteRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, removingId));
        buffer = PrefixIndexUtils.removeId(removingId, buffer);

        List<String> currentLexemes = new ArrayList<>(lexemes);
        try (DBIterator iterator = rocksDBProvider.createIterator(indexColumnFamily)) {
            KeyValue keyValue = iterator.seek(buildAttendantPrefixIndex());
            while (keyValue != null) {
                assertEquals(0, buffer, currentLexemes, keyValue);

                keyValue = iterator.next();
            }
        }

        Assertions.assertThat(currentLexemes.size()).isEqualTo(0);
    }

    @Test
    public void update() throws Exception {
        final int recordCount = 1;
        ByteBuffer buffer = createRecords(recordCount);

        final String newFileName = " Test sTrIng inform@mail. \r";
        recordSource.executeTransactional(transaction -> transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1, new String[]{"name"}, new Object[]{newFileName}));

        List<String> currentLexemes = new ArrayList<>(Arrays.asList("test", "string", "inform@mail.", "mail."));
        try (DBIterator iterator = rocksDBProvider.createIterator(indexColumnFamily)) {
            KeyValue keyValue = iterator.seek(buildAttendantPrefixIndex());
            while (keyValue != null) {
                assertEquals(0, buffer.array(), currentLexemes, keyValue);

                keyValue = iterator.next();
            }
        }

        Assertions.assertThat(currentLexemes.size()).isEqualTo(0);
    }

    private static void assertEquals(int expectedBlock, byte[] expectedValue, List<String> expectedLexemes, KeyValue actual) {
        PrefixIndexKey key = PrefixIndexKey.unpack(actual.getKey());

        Assertions.assertThat(expectedLexemes.remove(key.getLexeme())).isTrue();
        Assertions.assertThat(expectedBlock).isEqualTo(key.getBlockNumber());
        Assertions.assertThat(expectedValue).containsExactly(actual.getValue());
    }

    private ByteBuffer createRecords(int recordCount) throws Exception {
        ByteBuffer idsBuffer = TypeConvert.allocateBuffer(recordCount * Key.ID_BYTE_SIZE);
        recordSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; ++i) {
                long id = transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE, new String[]{"name"}, new Object[]{fileName});
                idsBuffer.putLong(id);
            }
        });

        return idsBuffer;
    }

    private KeyPattern buildAttendantPrefixIndex() {
        StructEntity structEntity = Schema.getEntity(StoreFileEditable.class);
        return PrefixIndexKey.buildKeyPatternForFind("", structEntity.getPrefixIndex(Collections.singleton(0)));
    }
}
