package org.forome.rocksdb;

import org.forome.database.anotation.Entity;
import org.forome.database.anotation.Field;
import org.forome.database.anotation.HashIndex;
import org.forome.database.domainobject.DomainObject;
import org.forome.database.exception.DatabaseException;

@Entity(
        namespace = "org.infomaximum.rocksdb",
        name = "record",
        fields = {
                @Field(number = RecordIndexReadable.FIELD_STRING_1, name = "str1", type = String.class),
                @Field(number = RecordIndexReadable.FIELD_LONG_1, name = "lng1", type = Long.class),
                @Field(number = RecordIndexReadable.FIELD_INT_1, name = "int1", type = Integer.class),
                @Field(number = RecordIndexReadable.FIELD_BOOLEAN_1, name = "bool1", type = Boolean.class)
        },
        hashIndexes = {
                @HashIndex(fields = {RecordIndexReadable.FIELD_STRING_1}),
                @HashIndex(fields = {RecordIndexReadable.FIELD_LONG_1}),
                @HashIndex(fields = {RecordIndexReadable.FIELD_LONG_1, RecordIndexReadable.FIELD_STRING_1})
        }
)
public class RecordIndexReadable  extends DomainObject {

    public final static int FIELD_STRING_1 = 0;
    public final static int FIELD_LONG_1 = 1;
    public final static int FIELD_INT_1 = 2;
    public final static int FIELD_BOOLEAN_1 = 3;

    public RecordIndexReadable(long id) {
        super(id);
    }

    public String getString1() throws DatabaseException {
        return getString(FIELD_STRING_1);
    }

    public Long getLong1() throws DatabaseException {
        return getLong(FIELD_LONG_1);
    }

    public Boolean getBoolean1() throws DatabaseException {
        return getBoolean(FIELD_BOOLEAN_1);
    }

    public Integer getInt1() throws DatabaseException {
        return getInteger(FIELD_INT_1);
    }
}
