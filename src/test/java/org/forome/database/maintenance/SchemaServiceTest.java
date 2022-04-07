package org.forome.database.maintenance;

import org.forome.database.exception.DatabaseException;
import org.forome.database.exception.TableNotFoundException;
import org.forome.database.schema.Schema;
import org.forome.domain.ExchangeFolderEditable;
import org.forome.domain.ExchangeFolderReadable;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.database.domainobject.DomainDataTest;
import org.junit.Assert;
import org.junit.Test;

public class SchemaServiceTest extends DomainDataTest {

    @Test
    public void validateValidScheme() throws DatabaseException {
        createDomain(ExchangeFolderReadable.class);
        createDomain(StoreFileReadable.class);

        new SchemaService(rocksDBProvider)
                .setNamespace("org.infomaximum.store")
                .setValidationMode(true)
                .setSchema(Schema.read(rocksDBProvider))
                .execute();

        Assert.assertTrue(true);
    }

//    @Test
//    public void validateInvalidScheme() throws DatabaseException {
//        try {
//            new SchemaService(rocksDBProvider)
//                    .setNamespace("org.infomaximum.store")
//                    .setValidationMode(true)
//                    .setSchema(Schema.read(rocksDBProvider))
//                    .execute();
//            Assert.fail();
//        } catch (InconsistentDatabaseException e) {
//            Assert.assertTrue(true);
//        }
//    }

    @Test
    public void removeInvalidScheme() throws DatabaseException {
        new SchemaService(rocksDBProvider)
                .setNamespace("org.infomaximum.store")
                .setChangeMode(ChangeMode.REMOVAL)
                .setValidationMode(false)
                .setSchema(Schema.read(rocksDBProvider))
                .execute();
    }

    @Test
    public void createAndValidateScheme() throws DatabaseException {
        new SchemaService(rocksDBProvider)
                .setNamespace("org.infomaximum.store")
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setSchema(Schema.read(rocksDBProvider))
                .execute();
        Assert.assertTrue(true);
    }

//    @Test
//    public void validateUnknownColumnFamily() throws Exception {
//        createDomain(ExchangeFolderEditable.class);
//        createDomain(StoreFileReadable.class);
//
//        rocksDBProvider.createColumnFamily("org.infomaximum.store.new_StoreFile.some_prefix");
//
//        try {
//            new SchemaService(rocksDBProvider)
//                    .setNamespace("org.infomaximum.store")
//                    .setValidationMode(true)
//                    .setSchema(Schema.read(rocksDBProvider))
//                    .execute();
//            Assert.fail();
//        } catch (InconsistentDatabaseException e) {
//            Assert.assertTrue(true);
//        }
//    }

    @Test
    public void validateWithIgnoringNotOwnedColumnFamily() {
        createDomain(ExchangeFolderReadable.class);
        createDomain(StoreFileReadable.class);

        rocksDBProvider.createColumnFamily("org.new_infomaximum.new_StoreFile.some_prefix");

        new SchemaService(rocksDBProvider)
                .setNamespace("org.infomaximum.store")
                .setValidationMode(true)
                .setSchema(Schema.read(rocksDBProvider))
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateInaccurateData() {
        try {
            createDomain(StoreFileEditable.class);
            Assert.fail();
        } catch (TableNotFoundException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void validateCoherentData() throws Exception {
        createDomain(ExchangeFolderEditable.class);
        createDomain(StoreFileEditable.class);

        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFolderId(folder.getId());
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            transaction.save(obj);
        });

        new SchemaService(rocksDBProvider)
                .setNamespace("org.infomaximum.store")
                .setValidationMode(true)
                .setSchema(Schema.read(rocksDBProvider))
                .execute();
    }
}
