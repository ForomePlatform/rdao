package org.forome.rocksdb;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by kris on 22.04.17.
 */
public abstract class RocksDataTest {

    protected Path pathDataBase;

    @Before
    public void init() throws Exception {
        pathDataBase = Files.createTempDirectory("rocksdb");
        pathDataBase.toAbsolutePath().toFile().deleteOnExit();
    }

    @After
    public void destroy() throws Exception {
        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
    }
}
