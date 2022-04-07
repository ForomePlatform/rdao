package org.forome.domain.proxy;

import org.forome.database.domainobject.DomainObjectEditable;
import org.forome.domain.type.FormatType;

/**
 * Created by user on 19.04.2017.
 */
public class ProxyStoreFileEditable extends ProxyStoreFileReadable implements DomainObjectEditable {

    public ProxyStoreFileEditable(long id) {
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

    public void setSingle(boolean single) {
        set(FIELD_SINGLE, single);
    }

    public void setFormat(FormatType format) {
        set(FIELD_FORMAT, format);
    }
}
