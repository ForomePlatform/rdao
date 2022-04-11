package org.forome.database.provider;

import org.forome.database.exception.DatabaseException;

public interface DBDataReader extends AutoCloseable {

    DBIterator createIterator(String columnFamily) throws DatabaseException;
    byte[] getValue(String columnFamily, byte[] key) throws DatabaseException;
}
