package org.forome.database.domainobject;

import org.forome.database.exception.DatabaseException;
import org.forome.database.utils.DomainBiConsumer;
import org.forome.rocksdb.RocksDBProvider;
import org.forome.rocksdb.RocksDataBaseBuilder;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class DomainDataInstanceTest extends DomainDataTest {

    private final static String TEMP_BD_DIR = "rocksdb_cur";

    protected RocksDBProvider rocksDBProvider = null;
    protected DomainObjectSource domainObjectSource = null;
    private Path pathDataBase = null;

    @Before
    public void setUp() throws Exception {
        closeEtalonBD();
        resetBDToEtalon();
    }

    @After
    public void tearDown() throws Exception {
        rocksDBProvider.close();
        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
        FileUtils.deleteDirectory(super.pathDataBase.toAbsolutePath().toFile());
    }

    protected void fillEtalonBD(DomainBiConsumer consumer) throws Exception {
        openEtalonBD();
        consumer.accept(super.domainObjectSource, super.rocksDBProvider);
        closeEtalonBD();
    }

    protected void resetBDToEtalon() throws IOException, DatabaseException {
        if (rocksDBProvider != null) {
            rocksDBProvider.close();
        }
        if (pathDataBase != null) {
            FileUtils.deleteDirectory(pathDataBase.toFile());
        } else {
            pathDataBase = Files.createTempDirectory(TEMP_BD_DIR);
            pathDataBase.toAbsolutePath().toFile().deleteOnExit();
        }
        FileUtils.copyDirectory(super.pathDataBase.toFile(), pathDataBase.toFile());
        rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        domainObjectSource = new DomainObjectSource(rocksDBProvider);
    }

    private void closeEtalonBD() {
        super.rocksDBProvider.close();
        super.rocksDBProvider = null;
        super.domainObjectSource = null;
    }

    private void openEtalonBD() throws DatabaseException {
        super.rocksDBProvider = new RocksDataBaseBuilder().withPath(super.pathDataBase).build();
        super.domainObjectSource = new DomainObjectSource(super.rocksDBProvider);
    }
}