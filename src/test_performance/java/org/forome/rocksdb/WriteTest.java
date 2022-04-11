package org.forome.rocksdb;

import org.forome.database.domainobject.DomainDataTest;
import org.forome.database.domainobject.Transaction;
import org.forome.rocksdb.util.PerfomanceTest;
import org.junit.Test;

public class WriteTest extends DomainDataTest {

    @Test
    public void createNonIndexedRecords1() throws Exception {
        createDomain(RecordReadable.class);

        PerfomanceTest.test(1000000, step-> {
            try (Transaction transaction = domainObjectSource.buildTransaction()) {
                RecordEditable rec = transaction.create(RecordEditable.class);
                rec.setString1("some value");
                transaction.save(rec);
                transaction.commit();
            }
        });
    }

    @Test
    public void createNonIndexedRecords2() throws Exception {
        createDomain(RecordReadable.class);

        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            PerfomanceTest.test(1000000, step -> {
                RecordEditable rec = transaction.create(RecordEditable.class);
                rec.setString1("some value");
                transaction.save(rec);
            });
            transaction.commit();
        }
    }

    @Test
    public void createIndexedRecords() throws Exception {
        createDomain(RecordIndexReadable.class);

        long counter[] = new long[] {0};

        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            PerfomanceTest.test(500000, step -> {
                RecordIndexEditable rec = transaction.create(RecordIndexEditable.class);
                long val = ++counter[0];
                rec.setString1(Long.toString(val));
                rec.setLong1(val);
                transaction.save(rec);
            });
            transaction.commit();
        }
    }
}
