package com.infomaximum.domain;

import com.infomaximum.database.domainobject.DomainObjectEditable;

public class DataTestEditable extends DataTestReadable implements DomainObjectEditable {

    public DataTestEditable(long id) {
        super(id);
    }

    public void setValue(String value) {
        set(FIELD_VALUE, value);
    }
}