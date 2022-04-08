package org.forome.domain;

import org.forome.database.anotation.*;
import org.forome.database.anotation.*;
import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.filter.RangeFilter;

@Entity(
        namespace = "org.forome.store",
        name = "StoreFile",
        fields = {
                @Field(number = BoundaryReadable.FIELD_LONG_1, name = "long_1", type = Long.class),
                @Field(number = BoundaryReadable.FIELD_LONG_2, name = "long_2", type = Long.class),
                @Field(number = BoundaryReadable.FIELD_LONG_3, name = "long_3", type = Long.class),
                @Field(number = BoundaryReadable.FIELD_STRING_1, name = "string_1", type = String.class),
                @Field(number = BoundaryReadable.FIELD_STRING_2, name = "string_2", type = String.class),
                @Field(number = BoundaryReadable.FIELD_STRING_3, name = "string_3", type = String.class),
        },
        hashIndexes = {
                @HashIndex(fields = {BoundaryReadable.FIELD_LONG_1}),
                @HashIndex(fields = {BoundaryReadable.FIELD_LONG_2}),
                @HashIndex(fields = {BoundaryReadable.FIELD_LONG_3})
        },
        prefixIndexes = {
                @PrefixIndex(fields = {BoundaryReadable.FIELD_STRING_1}),
                @PrefixIndex(fields = {BoundaryReadable.FIELD_STRING_2}),
                @PrefixIndex(fields = {BoundaryReadable.FIELD_STRING_3})
        },
        intervalIndexes = {
                @IntervalIndex(indexedField = BoundaryReadable.FIELD_LONG_1),
                @IntervalIndex(indexedField = BoundaryReadable.FIELD_LONG_2),
                @IntervalIndex(indexedField = BoundaryReadable.FIELD_LONG_3)
        },
        rangeIndexes = {
                @RangeIndex(beginField = BoundaryReadable.FIELD_LONG_1, endField = BoundaryReadable.FIELD_LONG_2),
                @RangeIndex(beginField = BoundaryReadable.FIELD_LONG_2, endField = BoundaryReadable.FIELD_LONG_3)
        }
)
public class BoundaryReadable extends DomainObject {

    public final static int FIELD_LONG_1=0;
    public final static int FIELD_LONG_2=1;
    public final static int FIELD_LONG_3=2;
    public final static int FIELD_STRING_1=3;
    public final static int FIELD_STRING_2=4;
    public final static int FIELD_STRING_3=5;

    public final static RangeFilter.IndexedField RANGE_LONG_FIELD = new RangeFilter.IndexedField(FIELD_LONG_1, FIELD_LONG_2);
    public final static RangeFilter.IndexedField RANGE_LONG_FIELD_2 = new RangeFilter.IndexedField(FIELD_LONG_2, FIELD_LONG_3);

    public BoundaryReadable(long id) {
        super(id);
    }

    public Long getLong1() {
        return getLong(FIELD_LONG_1);
    }

    public Long getLong2() {
        return getLong(FIELD_LONG_2);
    }

    public Long getLong3() {
        return getLong(FIELD_LONG_3);
    }

    public String getString1() {
        return getString(FIELD_STRING_1);
    }

    public String getString2() {
        return getString(FIELD_STRING_2);
    }

    public String getString3() {
        return getString(FIELD_STRING_3);
    }
}
