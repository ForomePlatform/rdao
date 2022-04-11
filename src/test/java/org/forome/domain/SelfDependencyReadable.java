package org.forome.domain;

import org.forome.database.anotation.Entity;
import org.forome.database.anotation.Field;
import org.forome.database.anotation.*;
import org.forome.database.domainobject.DomainObject;

@Entity(
        namespace = "org.forome.self",
        name = "SelfDependency",
        fields = {
                @Field(number = SelfDependencyReadable.FIELD_DEPENDENCE_ID, name = "dependence", type = Long.class, foreignDependency = SelfDependencyReadable.class),
        }
)
public class SelfDependencyReadable extends DomainObject {

    public final static int FIELD_DEPENDENCE_ID=0;

    public SelfDependencyReadable(long id) {
        super(id);
    }

    public Long getDependenceId() {
        return getLong(FIELD_DEPENDENCE_ID);
    }
}
