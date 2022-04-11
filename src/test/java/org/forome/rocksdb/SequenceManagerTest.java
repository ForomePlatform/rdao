package org.forome.rocksdb;

import org.forome.database.exception.SequenceAlreadyExistsException;
import org.forome.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class SequenceManagerTest extends RocksDataTest {

    @Test
    public void createNew() {
        String sequenceName = "sdfuisii";

        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDBProvider);
            sequenceManager.createSequence(sequenceName);

            Assert.assertEquals(1L, sequenceManager.getSequence(sequenceName).next());
        }
    }

    @Test
    public void createExisting() {
        String sequenceName = "sdfuisii";

        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDBProvider);
            sequenceManager.createSequence(sequenceName);

            try {
                sequenceManager.createSequence(sequenceName);
                Assert.fail();
            } catch (SequenceAlreadyExistsException e) {
                Assert.assertTrue(true);
            }
        }
    }

    @Test
    public void drop() {
        String sequenceName = "sdfuisii";

        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDBProvider);
            sequenceManager.createSequence(sequenceName);
            sequenceManager.getSequence(sequenceName).next();

            sequenceManager.dropSequence(sequenceName);

            sequenceManager.createSequence(sequenceName);
            Assert.assertEquals(1L, sequenceManager.getSequence(sequenceName).next());
        }
    }

    @Test
    public void order() {
        String sequenceName = "sdfuisii";

        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDBProvider);
            sequenceManager.createSequence(sequenceName);

            SequenceManager.Sequence sequence = sequenceManager.getSequence(sequenceName);
            for (long i = 1; i < 1000000; i++) {
                long id = sequence.next();
                Assert.assertEquals(i, id);
            }
        }
    }

    @Test
    public void restart() throws Exception {
        String sequenceName = "sdfuisii";
        Set<Long> ids = new HashSet<>();

        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            new SequenceManager(rocksDBProvider).createSequence(sequenceName);
        }

        startDbAndIncrementSequence(sequenceName, ids, 1);
        startDbAndIncrementSequence(sequenceName, ids, 3);
        startDbAndIncrementSequence(sequenceName, ids, 0);
        startDbAndIncrementSequence(sequenceName, ids, 7);
        startDbAndIncrementSequence(sequenceName, ids, 9);
        startDbAndIncrementSequence(sequenceName, ids, 10);
        startDbAndIncrementSequence(sequenceName, ids, 11);
        startDbAndIncrementSequence(sequenceName, ids, 99);
        startDbAndIncrementSequence(sequenceName, ids, 100);
        startDbAndIncrementSequence(sequenceName, ids, 101);
        for (int i=0; i<10; i++) {
            startDbAndIncrementSequence(sequenceName, ids, RandomUtil.random.nextInt(100) + 1);
        }
    }

    private void startDbAndIncrementSequence(String sequenceName, Set<Long> ids, int count) throws Exception {
        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDBProvider);

            SequenceManager.Sequence sequence = sequenceManager.getSequence(sequenceName);
            for (int i = 0; i < count; i++) {
                checkAndAddId(ids, sequence.next());
            }
        }
    }

    private static void checkAndAddId(Set<Long> ids, long id) {
        Assert.assertFalse("Conflict gen id", ids.contains(id));
        ids.add(id);
    }
}
