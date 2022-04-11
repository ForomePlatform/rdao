package org.forome.rocksdb;

import org.forome.database.domainobject.DomainDataTest;
import org.forome.database.maintenance.ChangeMode;
import org.forome.database.maintenance.SchemaService;
import org.forome.database.schema.Schema;
import org.forome.domain.ExchangeFolderEditable;
import org.forome.domain.StoreFileEditable;
import org.forome.rocksdb.util.PerfomanceTest;
import org.junit.Test;

public class SchemaServiceTest extends DomainDataTest {

    @Test
    public void validateCoherentData() throws Exception {
        createDomain(ExchangeFolderEditable.class);
        createDomain(StoreFileEditable.class);

        final int recordCount = 100 * 1000;

        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; ++i) {
                ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
                transaction.save(folder);

                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFolderId(folder.getId());
                transaction.save(obj);
            }
        });

        SchemaService schemaService = new SchemaService(rocksDBProvider)
                .setNamespace("org.forome.store")
                .setChangeMode(ChangeMode.NONE)
                .setSchema(Schema.read(rocksDBProvider));

        PerfomanceTest.test(10, step -> schemaService.execute());
    }
}
