package org.forome.database.utils;

import org.forome.database.domainobject.DomainObjectSource;
import org.forome.rocksdb.RocksDBProvider;

@FunctionalInterface
public interface DomainBiConsumer {

    void accept(DomainObjectSource domainObjectSource, RocksDBProvider rocksDBProvider) throws Exception;
}