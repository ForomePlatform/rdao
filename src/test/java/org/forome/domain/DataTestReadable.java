package org.forome.domain;

import org.forome.database.anotation.Entity;
import org.forome.database.anotation.Field;
import org.forome.database.domainobject.DomainObject;

@Entity(
        namespace = "org.forome.exchange",
        name = "dataTest",
        fields = {
                @Field(number = DataTestReadable.FIELD_VALUE, name = "value", type = String.class)
        }
)
public class DataTestReadable extends DomainObject {
    public final static int FIELD_VALUE = 0;

    public DataTestReadable(long id){
        super(id);
    }

    public String getValue() {
        return getString(FIELD_VALUE);
    }
}