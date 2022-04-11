package org.forome.rocksdb;

import org.forome.database.exception.ColumnFamilyNotFoundException;
import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.DBProvider;
import org.forome.database.provider.DBTransaction;
import org.forome.database.utils.TypeConvert;
import org.rocksdb.*;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class RocksDBProvider implements DBProvider, AutoCloseable {

    public static final String DEFAULT_COLUMN_FAMILY = new String(RocksDB.DEFAULT_COLUMN_FAMILY);

    private final OptimisticTransactionDB rocksDB;
    private final ConcurrentMap<String, ColumnFamilyHandle> columnFamilies;
    private final WriteOptions writeOptions = new WriteOptions();
    private final ReadOptions readOptions = new ReadOptions();
    private final SequenceManager sequenceManager;

    RocksDBProvider(OptimisticTransactionDB rocksDB, ConcurrentMap<String, ColumnFamilyHandle> columnFamilies) throws DatabaseException {
        this.rocksDB = rocksDB;
        this.columnFamilies = columnFamilies;
        this.sequenceManager = new SequenceManager(this);
    }

    public RocksDB getRocksDB() {
        return rocksDB;
    }

    @Override
    public DBTransaction beginTransaction() throws DatabaseException {
        return new RocksDBTransaction(rocksDB.beginTransaction(writeOptions), this);
    }

    @Override
    public DBIterator createIterator(String columnFamily) throws DatabaseException {
        return new RocksDBIterator(getRocksDB().newIterator(getColumnFamilyHandle(columnFamily), readOptions));
    }

    @Override
    public byte[] getValue(String columnFamily, final byte[] key) throws DatabaseException {
        try {
            return getRocksDB().get(getColumnFamilyHandle(columnFamily), readOptions, key);
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean containsColumnFamily(String name) throws DatabaseException {
        return columnFamilies.containsKey(name);
    }

    @Override
    public String[] getColumnFamilies() {
        int size = columnFamilies.size();
        if (columnFamilies.containsKey(RocksDBProvider.DEFAULT_COLUMN_FAMILY)) {
            --size;
        }

        String[] columns = new String[size];
        int pos = 0;
        for (Map.Entry<String, ColumnFamilyHandle> cf : columnFamilies.entrySet()) {
            if (!cf.getKey().equals(RocksDBProvider.DEFAULT_COLUMN_FAMILY)) {
                columns[pos++] = cf.getKey();
            }
        }

        return columns;
    }

    @Override
    public boolean containsSequence(String name) throws DatabaseException {
        return sequenceManager.getSequence(name) != null;
    }

    @Override
    public void createSequence(String name) throws DatabaseException {
        sequenceManager.createSequence(name);
    }

    @Override
    public void dropSequence(String name) throws DatabaseException {
        sequenceManager.dropSequence(name);
    }

    @Override
    public void createColumnFamily(String columnFamilyName) throws DatabaseException {
        try {
            ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(TypeConvert.pack(columnFamilyName));
            ColumnFamilyHandle columnFamilyHandle = getRocksDB().createColumnFamily(columnFamilyDescriptor);
            if (columnFamilies.putIfAbsent(columnFamilyName, columnFamilyHandle) != null) {
                try (ColumnFamilyHandle handle = columnFamilyHandle) {
                    getRocksDB().dropColumnFamily(handle);
                }
            }
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void dropColumnFamily(String columnFamilyName) throws DatabaseException {
        try (ColumnFamilyHandle columnFamilyHandle = columnFamilies.remove(columnFamilyName)) {
            if (columnFamilyHandle != null) {
                getRocksDB().dropColumnFamily(columnFamilyHandle);
            }
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void compactRange() throws DatabaseException {
        try {
            rocksDB.compactRange();
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() {
        List<AbstractNativeReference> refs = new ArrayList<>(columnFamilies.size() + 3);
        refs.add(readOptions);
        refs.add(writeOptions);
        for (Map.Entry<String, ColumnFamilyHandle> entry : columnFamilies.entrySet()) {
            refs.add(entry.getValue());
        }
        refs.add(rocksDB);

        for (int i = 0; i < refs.size(); ++i) {
            try {
                refs.get(i).close();
            } catch (Throwable e) {
                try {
                    for (++i; i < refs.size(); ++i) {
                        refs.get(i).close();
                    }
                } catch (Throwable ignore) {}

                throw e;
            }
        }
    }

    ColumnFamilyHandle getColumnFamilyHandle(String columnFamilyName) throws ColumnFamilyNotFoundException {
        ColumnFamilyHandle cf = columnFamilies.get(columnFamilyName);
        if (cf != null) {
            return cf;
        }
        throw new ColumnFamilyNotFoundException(columnFamilyName);
    }

    WriteOptions getWriteOptions() {
        return writeOptions;
    }

    ReadOptions getReadOptions() {
        return readOptions;
    }

    SequenceManager getSequenceManager() {
        return sequenceManager;
    }
}
