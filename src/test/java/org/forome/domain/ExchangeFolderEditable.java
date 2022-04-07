package org.forome.domain;

import org.forome.database.domainobject.DomainObjectEditable;
import org.forome.database.exception.DatabaseException;

import java.time.Instant;

/**
 * Created by kris on 27.06.17.
 */
public class ExchangeFolderEditable extends ExchangeFolderReadable implements DomainObjectEditable {

    public ExchangeFolderEditable(long id){
        super(id);
    }

    public void setUuid(String uuid) {
        set(FIELD_UUID, uuid);
    }

    public void setUserEmail(String userEmail) {
        set(FIELD_USER_EMAIL, userEmail);
    }

    public void setSyncDate(Instant syncInstant) {
        set(FIELD_SYNC_DATE, syncInstant);
    }

    public void setSyncState(String syncState) {
        set(FIELD_SYNC_STATE, syncState);
    }

    public void setParentId(Long parentId) throws DatabaseException {
        set(FIELD_PARENT_ID, parentId);
    }
}