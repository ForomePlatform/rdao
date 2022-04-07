package org.forome.database.domainobject;

import org.forome.database.provider.DBIterator;
import org.forome.database.provider.DBProvider;

import org.forome.database.exception.DatabaseException;
import org.forome.database.schema.StructEntity;

public class DomainObjectSource extends DataEnumerable {

    @FunctionalInterface
    public interface Monad {

        /**
         * Реализация операции.
         * @param transaction Контекст, в котором выполняется операция.
         * @throws Exception Если во время выполнения операции возникла ошибка.
         */
        void action(final Transaction transaction) throws Exception;
    }

    @FunctionalInterface
    public interface Function<R> {

        /**
         * Реализация операции.
         * @param transaction Контекст, в котором выполняется операция.
         * @throws Exception Если во время выполнения операции возникла ошибка.
         */
        R apply(final Transaction transaction) throws Exception;
    }

    public DomainObjectSource(DBProvider dbProvider) {
        super(dbProvider);
    }

    public void executeTransactional(final Monad operation) throws Exception {
        try (Transaction transaction = buildTransaction()) {
            operation.action(transaction);
            transaction.commit();
        }
    }

    public <R> R executeFunctionTransactional(final Function<R> operation) throws Exception {
        try (Transaction transaction = buildTransaction()) {
            R result = operation.apply(transaction);
            transaction.commit();
            return result;
        }
    }

    public Transaction buildTransaction() {
        return new Transaction(getDbProvider());
    }

    @Override
    public DBIterator createIterator(String columnFamily) throws DatabaseException {
        return getDbProvider().createIterator(columnFamily);
    }

    @Override
    public boolean isMarkedForDeletion(StructEntity entity, long objId) {
        return false;
    }
}
