package org.forome.database.domainobject.iterator;

import org.forome.database.domainobject.DomainObject;

import org.forome.database.exception.DatabaseException;

/**
 * Created by kris on 08.09.17.
 */
public interface IteratorEntity<E extends DomainObject> extends AutoCloseable {

    boolean hasNext();

    E next() throws DatabaseException;

    void close() throws DatabaseException;

}
