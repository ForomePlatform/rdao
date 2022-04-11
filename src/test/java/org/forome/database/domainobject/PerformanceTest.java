package org.forome.database.domainobject;

import org.forome.database.domainobject.filter.HashFilter;
import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.domain.GeneralEditable;
import org.forome.rocksdb.RocksDBProvider;
import org.forome.util.DurationUtils;
import org.junit.Assert;

import java.time.Duration;

public class PerformanceTest extends DomainDataInstanceTest {

    /**
     * Чтение в одной транзакции до удаления: 1 s 1 ms
     * В одной транзакции удаление, чтение и коммит: 680 ms
     * Чтение в другой транзакции: 568 ms
     */
//    @Test
    public void readTest() throws Exception {
        final int objectsCount = 100_000;
        final int deletingCount = 70_000;

        fillEtalonBD((d, r) -> createObjects(objectsCount, d, r));
        resetBDToEtalon();

        System.out.print("Чтение в одной транзакции до удаления: ");
        domainObjectSource.executeTransactional(transaction -> assertRead(1, objectsCount, transaction, objectsCount));

        System.out.print("В одной транзакции удаление, чтение и коммит: ");
        resetBDToEtalon();
        domainObjectSource.executeTransactional(transaction -> {
            deleteObjects(deletingCount, transaction);
            assertRead(1, objectsCount, transaction, objectsCount - deletingCount);
        });

        System.out.print("Чтение в другой транзакции: ");
        domainObjectSource.executeTransactional(transaction -> assertRead(1, objectsCount, transaction, objectsCount - deletingCount));
    }

    private void createObjects(long count, DomainObjectSource domainObjectSource, RocksDBProvider rocksDBProvider) throws Exception {
        createDomain(GeneralEditable.class, rocksDBProvider);
        domainObjectSource.executeTransactional(transaction -> {
            for (long i = 1; i <= count; i++) {
                GeneralEditable generalEditable = transaction.create(GeneralEditable.class);
                generalEditable.setValue(i + count);
                transaction.save(generalEditable);
            }
        });

        domainObjectSource.executeTransactional(transaction -> {
            for (long i = 1; i <= count; i++) {
                GeneralEditable generalEditable = transaction.get(GeneralEditable.class, i);
                generalEditable.setValue(i);
                transaction.save(generalEditable);
            }
        });
    }

    private void deleteObjects(long count, Transaction transaction) {
        for (long i = 1; i <= count; i++) {
            GeneralEditable generalEditable = transaction.get(GeneralEditable.class, i);
            transaction.remove(generalEditable);
        }
    }

    private void assertRead(long beginId, long endId, Transaction transaction, int expectedRecCount) throws Exception {
        long beginTime = System.nanoTime();
        int recCount = 0;
        for (int j = 0; j < 3; ++j) {
            for (; beginId <= endId; ++beginId) {
                try (IteratorEntity<GeneralEditable> it = transaction.find(GeneralEditable.class, new HashFilter(GeneralEditable.FIELD_VALUE, beginId))) {
                    if (it.hasNext()) {
                        ++recCount;
                    }
                }
            }
        }
        Duration duration = Duration.ofNanos(System.nanoTime() - beginTime);
        System.out.println(DurationUtils.toString(duration));
        Assert.assertTrue(duration.compareTo(Duration.ofMillis(1500)) < 0);
        Assert.assertEquals(expectedRecCount, recCount);
    }
}