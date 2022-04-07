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

        rocksDBProvider.createColumnFamily("org.infomaximum.database.exception");
        rocksDBProvider.createColumnFamily("org.infomaximum.database.maintenance");

        rocksDBProvider.createColumnFamily("org.infomaximum.rocksdb.exception");
        rocksDBProvider.createColumnFamily("org.infomaximum.rocksdb.maintenance");

        new NamespaceValidator(rocksDBProvider)
                .withNamespace("org.infomaximum.database")
                .withNamespace("org.infomaximum.rocksdb")
                .withNamespace("service")
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateInvalidSchema() throws DatabaseException {

        rocksDBProvider.createColumnFamily("org.infomaximum.database.exception");
        rocksDBProvider.createColumnFamily("org.infomaximum.database.maintenance");

        rocksDBProvider.createColumnFamily("org.infomaximum.rocksdb.exception");
        rocksDBProvider.createColumnFamily("org.infomaximum.rocksdb.maintenance");

        rocksDBProvider.createColumnFamily("org.infomaximum.maintenance");

        try {
            new NamespaceValidator(rocksDBProvider)
                    .withNamespace("org.infomaximum.database")
                    .withNamespace("org.infomaximum.rocksdb")
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }
}
