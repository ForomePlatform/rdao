package org.forome.database.domainobject;

import org.forome.domain.ExchangeFolderReadable;
import org.junit.Before;

public abstract class ExchangeFolderDataTest extends DomainDataTest {

    @Before
    public void init() throws Exception {
        super.init();

        createDomain(ExchangeFolderReadable.class);
    }
}
