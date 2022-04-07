package org.forome.domain;

import org.forome.database.domainobject.DomainObjectEditable;

public class SelfDependencyEditable extends SelfDependencyReadable implements DomainObjectEditable {

    public SelfDependencyEditable(long id) {
        super(id);
    }

    public void setDependenceId(long dependenceId) {
        set(FIELD_DEPENDENCE_ID, dependenceId);
    }
}
