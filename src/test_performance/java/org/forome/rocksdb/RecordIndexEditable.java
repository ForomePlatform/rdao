package org.forome.rocksdb;

import org.forome.database.domainobject.DomainObjectEditable;

public class RecordIndexEditable extends RecordIndexReadable implements DomainObjectEditable {

    public RecordIndexEditable(long id) {
        super(id);
    }

    void setString1(String value) {
        set(FIELD_STRING_1, value);
    }

    void setLong1(long value) {
        set(FIELD_LONG_1, value);
    }
}