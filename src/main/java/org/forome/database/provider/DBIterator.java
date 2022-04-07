package org.forome.database.provider;

import org.forome.database.exception.DatabaseException;

public interface DBIterator extends AutoCloseable {

    enum StepDirection {
        FORWARD, BACKWARD
    }

    KeyValue seek(KeyPattern pattern) throws DatabaseException;
    KeyValue next() throws DatabaseException;
    KeyValue step(StepDirection direction) throws DatabaseException;

    @Override
    void close() throws DatabaseException;
}
