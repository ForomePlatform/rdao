package org.forome.domain;

import org.forome.database.domainobject.DomainObjectEditable;

public class BoundaryEditable extends BoundaryReadable implements DomainObjectEditable {

    public BoundaryEditable(long id) {
        super(id);
    }

    public void setLong1(Long value) {
        set(FIELD_LONG_1, value);
    }

    public void setLong2(Long value) {
        set(FIELD_LONG_2, value);
    }

    public void setLong3(Long value) {
        set(FIELD_LONG_3, value);
    }

    public void setString1(String value) {
        set(FIELD_STRING_1, value);
    }

    public void setString2(String value) {
        set(FIELD_STRING_2, value);
    }

    public void setString3(String value) {
        set(FIELD_STRING_3, value);
    }
}
