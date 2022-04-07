package org.forome.domain;

import org.forome.database.domainobject.DomainObjectEditable;

public class GeneralEditable extends GeneralReadable implements DomainObjectEditable {

    public GeneralEditable(long id) {
        super(id);
    }

    public void setValue(Long value) {
        set(FIELD_VALUE, value);
    }
}