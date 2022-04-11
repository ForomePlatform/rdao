package org.forome.domain;

import org.forome.database.domainobject.DomainObjectEditable;
import org.forome.domain.type.FormatType;

import java.time.Instant;
import java.time.LocalDateTime;

public class StoreFileEditable extends StoreFileReadable implements DomainObjectEditable {

    public StoreFileEditable(long id) {
        super(id);
    }

    public void setFileName(String fileName) {
        set(FIELD_FILE_NAME, fileName);
    }

    public void setContentType(String contentType) {
        set(FIELD_CONTENT_TYPE, contentType);
    }

    public void setSize(long size) {
        set(FIELD_SIZE, size);
    }

    public void setSingle(Boolean single) {
        set(FIELD_SINGLE, single);
    }

    public void setFormat(FormatType format) {
        set(FIELD_FORMAT, format);
    }

    public void setFolderId(long folderId) {
        set(FIELD_FOLDER_ID, folderId);
    }

    public void setFolderId(Long folderId) {
        set(FIELD_FOLDER_ID, folderId);
    }

    public void setDouble(Double value) {
        set(FIELD_DOUBLE, value);
    }

    public void setBeginTime(Instant value) {
        set(FIELD_BEGIN_TIME, value);
    }

    public void setEndTime(Instant value) {
        set(FIELD_END_TIME, value);
    }

    public void setBegin(Long value) {
        set(FIELD_BEGIN, value);
    }

    public void setEnd(Long value) {
        set(FIELD_END, value);
    }

    public void setLocalBegin(LocalDateTime value) {
        set(FIELD_LOCAL_BEGIN, value);
    }

    public void setLocalEnd(LocalDateTime value) {
        set(FIELD_LOCAL_END, value);
    }

    public void setData(byte[] data) {
        set(FIELD_DATA, data);
    }
}
