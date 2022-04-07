package org.forome.database.domainobject.engine;

import org.forome.database.RecordSource;
import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.domain.SelfDependencyReadable;
import org.junit.jupiter.api.Test;


public class SelfDependencyTest extends StoreFileDataTest {

    @Test
    public void dependencyOnOtherObject() throws Exception {
        createDomain(SelfDependencyReadable.class);
        recordSource = new RecordSource(rocksDBProvider);

        long recordId = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord("SelfDependency", "org.infomaximum.self", new String[] {}, new Object[] {}));
        recordSource.executeTransactional(transaction -> {
            transaction.insertRecord("SelfDependency", "org.infomaximum.self", new Object[] {recordId});
        });
    }
    
    @Test
    public void updateDependencyOnTheObject() throws Exception {
        createDomain(SelfDependencyReadable.class);
        recordSource = new RecordSource(rocksDBProvider);

        long recordId = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord("SelfDependency", "org.infomaximum.self", new String[] {}, new Object[] {}));
        recordSource.executeTransactional(transaction -> {
            transaction.updateRecord("SelfDependency", "org.infomaximum.self", recordId, new String[] {"dependence"}, new Object[] {recordId});
        });
    }
}
