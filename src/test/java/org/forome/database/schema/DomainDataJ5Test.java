package org.forome.database.schema;

import org.forome.database.domainobject.DomainObjectSource;
import org.forome.database.domainobject.DomainObject;
import org.forome.database.exception.DatabaseException;
import org.forome.database.maintenance.ChangeMode;
import org.forome.database.maintenance.DomainService;
import org.forome.rocksdb.RocksDBProvider;
import org.forome.rocksdb.RocksDataBaseBuilder;
import org.forome.rocksdb.RocksDataTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;


public abstract class DomainDataJ5Test extends RocksDataTest {

    protected RocksDBProvider rocksDBProvider;

    protected DomainObjectSource domainObjectSource;

    @BeforeEach
    public void init() throws Exception {
        super.init();

        rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        domainObjectSource = new DomainObjectSource(rocksDBProvider);
    }

    @AfterEach
    public void destroy() throws Exception {
        if (rocksDBProvider != null) {
            rocksDBProvider.close();
        }

        super.destroy();
    }

    protected void createDomain(Class<? extends DomainObject> clazz) throws DatabaseException {
        createDomain(clazz, rocksDBProvider);
    }

    protected void createDomain(Class<? extends DomainObject> clazz, RocksDBProvider rocksDBProvider) throws DatabaseException {
        Schema schema = Schema.read(rocksDBProvider);
        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(clazz))
                .execute();
    }
}
