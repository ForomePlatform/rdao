package org.forome.database;

import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.DBTransaction;
import org.forome.database.schema.dbstruct.DBSchema;

public class Transaction implements AutoCloseable {

    private final DBTransaction dbTransaction;
    private final DataCommand dataCommand;

    Transaction(DBTransaction dbTransaction, DBSchema schema) {
        this.dbTransaction = dbTransaction;
        this.dataCommand = new DataCommand(dbTransaction, schema);
    }

    public DataCommand getCommand() {
        return dataCommand;
    }

    public void commit() throws DatabaseException {
        dbTransaction.commit();
    }

    public void rollback() throws DatabaseException {
        dbTransaction.rollback();
    }

    @Override
    public void close() throws DatabaseException {
        dbTransaction.close();
    }
}
