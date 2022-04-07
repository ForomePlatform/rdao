package org.forome.database.domainobject;

import org.forome.domain.DataTestEditable;
import org.forome.rocksdb.RocksDBProvider;
import org.junit.Assert;
import org.junit.Test;

public class DomainDataInstanceTestTest extends DomainDataInstanceTest {

    @Test
    public void test() throws Exception {
        fillEtalonBD(this::fillData);
        resetBDToEtalon();
        domainObjectSource.executeTransactional(transaction -> {
            DataTestEditable dataTestEditable = transaction.get(DataTestEditable.class, 1L);
            Assert.assertNotNull(dataTestEditable);
            transaction.remove(dataTestEditable);
        });

        resetBDToEtalon();

        domainObjectSource.executeTransactional(transaction -> {
            DataTestEditable dataTestEditable = transaction.get(DataTestEditable.class, 1L);
            Assert.assertNotNull(dataTestEditable);
            transaction.remove(dataTestEditable);
        });

        domainObjectSource.executeTransactional(transaction -> {
            DataTestEditable dataTestEditable = transaction.get(DataTestEditable.class, 1L);
            Assert.assertNull(dataTestEditable);
        });
    }

    private void fillData(DomainObjectSource domainObjectSource, RocksDBProvider provider) throws Exception {
        createDomain(DataTestEditable.class, provider);
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < 10; ++i) {
                DataTestEditable generalEditable = transaction.create(DataTestEditable.class);
                generalEditable.setValue("value:" + i);
                transaction.save(generalEditable);
            }
        });
    }
}