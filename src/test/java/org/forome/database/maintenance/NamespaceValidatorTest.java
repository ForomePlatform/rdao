package org.forome.database.maintenance;

import org.forome.database.exception.DatabaseException;
import org.forome.database.exception.InconsistentDatabaseException;
import org.forome.database.domainobject.DomainDataTest;
import org.junit.Assert;
import org.junit.Test;

public class NamespaceValidatorTest extends DomainDataTest {

    @Test
    public void validateEmptySchema() throws DatabaseException {
        new NamespaceValidator(rocksDBProvider).execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateValidSchema() throws DatabaseException {

        rocksDBProvider.createColumnFamily("org.forome.database.exception");
        rocksDBProvider.createColumnFamily("org.forome.database.maintenance");

        rocksDBProvider.createColumnFamily("org.forome.rocksdb.exception");
        rocksDBProvider.createColumnFamily("org.forome.rocksdb.maintenance");

        new NamespaceValidator(rocksDBProvider)
                .withNamespace("org.forome.database")
                .withNamespace("org.forome.rocksdb")
                .withNamespace("service")
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateInvalidSchema() throws DatabaseException {

        rocksDBProvider.createColumnFamily("org.forome.database.exception");
        rocksDBProvider.createColumnFamily("org.forome.database.maintenance");

        rocksDBProvider.createColumnFamily("org.forome.rocksdb.exception");
        rocksDBProvider.createColumnFamily("org.forome.rocksdb.maintenance");

        rocksDBProvider.createColumnFamily("org.forome.maintenance");

        try {
            new NamespaceValidator(rocksDBProvider)
                    .withNamespace("org.forome.database")
                    .withNamespace("org.forome.rocksdb")
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }
}
