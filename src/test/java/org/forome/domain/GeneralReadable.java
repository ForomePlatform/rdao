package org.forome.domain;

import org.forome.database.anotation.Entity;
import org.forome.database.anotation.Field;
import org.forome.database.anotation.HashIndex;
import org.forome.database.domainobject.DomainObject;

@Entity(
        namespace = "org.infomaximum.rocksdb",
        name = "general",
        fields = {
                @Field(number = GeneralReadable.FIELD_VALUE, name = "value", type = Long.class),
        },
        hashIndexes = {
                @HashIndex(fields = {GeneralReadable.FIELD_VALUE})
        }
)
public class GeneralReadable extends DomainObject {

    public final static int FIELD_VALUE = 0;

    public GeneralReadable(long id) {
        super(id);
    }

    public Long getValue() {
        return getLong(FIELD_VALUE);
    }
}