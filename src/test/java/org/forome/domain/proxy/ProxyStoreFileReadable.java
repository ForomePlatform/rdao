package org.forome.domain.proxy;

import org.forome.database.anotation.Entity;
import org.forome.database.anotation.Field;
import org.forome.database.anotation.HashIndex;
import org.forome.domain.type.FormatType;

/**
 * Created by user on 19.04.2017.
 */
@Entity(
        namespace = "org.forome.store",
        name = "StoreFile",
        fields = {
                @Field(number = ProxyStoreFileReadable.FIELD_FILE_NAME, name = "name", type = String.class),
                @Field(number = ProxyStoreFileReadable.FIELD_CONTENT_TYPE, name = "type", type = String.class),
                @Field(number = ProxyStoreFileReadable.FIELD_SIZE, name = "size", type = Long.class),
                @Field(number = ProxyStoreFileReadable.FIELD_SINGLE, name = "single", type = Boolean.class),
                @Field(number = ProxyStoreFileReadable.FIELD_FORMAT, name = "format", type = FormatType.class)
        },
        hashIndexes = {
                @HashIndex(fields = {ProxyStoreFileReadable.FIELD_SIZE}),
                @HashIndex(fields = {ProxyStoreFileReadable.FIELD_SIZE, ProxyStoreFileReadable.FIELD_FILE_NAME})
        }
)
public class ProxyStoreFileReadable extends ProxyDomainObject {

    public final static int FIELD_FILE_NAME = 0;
    public final static int FIELD_CONTENT_TYPE = 1;
    public final static int FIELD_SIZE = 2;
    public final static int FIELD_SINGLE = 3;
    public final static int FIELD_FORMAT = 4;


    public ProxyStoreFileReadable(long id) {
        super(id);
    }


    public String getFileName() {
        return getString(FIELD_FILE_NAME);
    }

    public String getContentType() {
        return getString(FIELD_CONTENT_TYPE);
    }

    public long getSize() {
        return getLong(FIELD_SIZE);
    }

    public boolean isSingle() {
        return getBoolean(FIELD_SINGLE);
    }

    public FormatType getFormat() {
        return get(FIELD_FORMAT);
    }
}
