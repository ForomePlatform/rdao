package org.forome.rocksdb;

import org.forome.database.exception.DatabaseException;
import org.forome.database.utils.PathUtils;
import org.forome.database.utils.TempLibraryCleaner;
import org.forome.database.utils.TypeConvert;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RocksDataBaseBuilder {

    private Path path;

    public RocksDataBaseBuilder withPath(Path path) {
        this.path = path.toAbsolutePath();
        return this;
    }

    public RocksDBProvider build() throws DatabaseException {
        TempLibraryCleaner.clear();
        PathUtils.checkPath(path);
        try (DBOptions options = buildOptions()) {
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = getColumnFamilyDescriptors();

            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            OptimisticTransactionDB rocksDB = OptimisticTransactionDB.open(options, path.toString(), columnFamilyDescriptors, columnFamilyHandles);

            ConcurrentMap<String, ColumnFamilyHandle> columnFamilies = new ConcurrentHashMap<>();
            for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
                String columnFamilyName = TypeConvert.unpackString(columnFamilyDescriptors.get(i).getName());
                ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
                columnFamilies.put(columnFamilyName, columnFamilyHandle);
            }

            return new RocksDBProvider(rocksDB, columnFamilies);
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    private DBOptions buildOptions() throws RocksDBException {
        final String optionsFilePath = path.toString() + ".ini";

        DBOptions options = new DBOptions();
        if (Files.exists(Paths.get(optionsFilePath))) {
            final List<ColumnFamilyDescriptor> ignoreDescs = new ArrayList<>();
            OptionsUtil.loadOptionsFromFile(optionsFilePath, Env.getDefault(), options, ignoreDescs, false);
        } else {
            options
                    .setInfoLogLevel(InfoLogLevel.WARN_LEVEL)
                    .setMaxTotalWalSize(100L * SizeUnit.MB);
        }

        return options.setCreateIfMissing(true);
    }

    private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors() throws RocksDBException {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        try (Options options = new Options()) {
            for (byte[] columnFamilyName : RocksDB.listColumnFamilies(options, path.toString())) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(columnFamilyName));
            }
        }

        if (columnFamilyDescriptors.isEmpty()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(TypeConvert.pack(RocksDBProvider.DEFAULT_COLUMN_FAMILY)));
        }

        return columnFamilyDescriptors;
    }
}
