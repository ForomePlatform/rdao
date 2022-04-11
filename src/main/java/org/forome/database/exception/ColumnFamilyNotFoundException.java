package org.forome.database.exception;

public class ColumnFamilyNotFoundException extends DatabaseException {

    public ColumnFamilyNotFoundException(String columnFamily) {
        super("Column family " + columnFamily + " not found.");
    }
}
