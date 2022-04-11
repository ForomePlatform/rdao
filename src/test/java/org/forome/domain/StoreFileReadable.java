package org.forome.domain;

import org.forome.database.anotation.*;
import org.forome.database.anotation.*;
import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.filter.RangeFilter;
import org.forome.database.utils.EnumConverter;
import org.forome.domain.type.FormatType;

import java.time.Instant;
import java.time.LocalDateTime;

/**
 * Created by user on 19.04.2017.
 */
@Entity(
        namespace = "org.forome.store",
        name = "StoreFile",
        fields = {
                @Field(number = StoreFileReadable.FIELD_FILE_NAME, name = "name", type = String.class),
                @Field(number = StoreFileReadable.FIELD_CONTENT_TYPE, name = "type", type = String.class),
                @Field(number = StoreFileReadable.FIELD_SIZE, name = "size", type = Long.class),
                @Field(number = StoreFileReadable.FIELD_SINGLE, name = "single", type = Boolean.class),
                @Field(number = StoreFileReadable.FIELD_FORMAT, name = "format", type = FormatType.class, packerType = StoreFileReadable.FormatConverter.class),
                @Field(number = StoreFileReadable.FIELD_FOLDER_ID, name = "folder_id", type = Long.class, foreignDependency = ExchangeFolderReadable.class),
                @Field(number = StoreFileReadable.FIELD_DOUBLE, name = "double", type = Double.class),
                @Field(number = StoreFileReadable.FIELD_BEGIN_TIME, name = "begin_time", type = Instant.class),
                @Field(number = StoreFileReadable.FIELD_END_TIME, name = "end_time", type = Instant.class),
                @Field(number = StoreFileReadable.FIELD_BEGIN, name = "begin", type = Long.class),
                @Field(number = StoreFileReadable.FIELD_END, name = "end", type = Long.class),
                @Field(number = StoreFileReadable.FIELD_LOCAL_BEGIN, name = "local_begin", type = LocalDateTime.class),
                @Field(number = StoreFileReadable.FIELD_LOCAL_END, name = "local_end", type = LocalDateTime.class),
                @Field(number = StoreFileReadable.FIELD_DATA, name = "data", type = byte[].class)
        },
        hashIndexes = {
                @HashIndex(fields = {StoreFileReadable.FIELD_SIZE}),
                @HashIndex(fields = {StoreFileReadable.FIELD_FILE_NAME}),
                @HashIndex(fields = {StoreFileReadable.FIELD_SIZE, StoreFileReadable.FIELD_FILE_NAME}),
                @HashIndex(fields = {StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SINGLE}),
                @HashIndex(fields = {StoreFileReadable.FIELD_FORMAT}),
                @HashIndex(fields = {StoreFileReadable.FIELD_LOCAL_BEGIN})
        },
        prefixIndexes = {
                @PrefixIndex(fields = {StoreFileReadable.FIELD_FILE_NAME}),
                @PrefixIndex(fields = {StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_CONTENT_TYPE})
        },
        intervalIndexes = {
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_SIZE),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_DOUBLE),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_BEGIN_TIME),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_LOCAL_BEGIN),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_SIZE, hashedFields = {StoreFileReadable.FIELD_FILE_NAME}),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_SIZE, hashedFields = {StoreFileReadable.FIELD_FOLDER_ID})
        },
        rangeIndexes = {
                @RangeIndex(beginField = StoreFileReadable.FIELD_BEGIN, endField = StoreFileReadable.FIELD_END),
                @RangeIndex(beginField = StoreFileReadable.FIELD_BEGIN, endField = StoreFileReadable.FIELD_END, hashedFields = {StoreFileReadable.FIELD_FOLDER_ID}),
                @RangeIndex(beginField = StoreFileReadable.FIELD_BEGIN_TIME, endField = StoreFileReadable.FIELD_END_TIME),
                @RangeIndex(beginField = StoreFileReadable.FIELD_BEGIN_TIME, endField = StoreFileReadable.FIELD_END_TIME, hashedFields = {StoreFileReadable.FIELD_FOLDER_ID}),
                @RangeIndex(beginField = StoreFileReadable.FIELD_LOCAL_BEGIN, endField = StoreFileReadable.FIELD_LOCAL_END)
        }
)
public class StoreFileReadable extends DomainObject {

    public final static int FIELD_FILE_NAME=0;
    public final static int FIELD_CONTENT_TYPE=1;
    public final static int FIELD_SIZE=2;
    public final static int FIELD_SINGLE=3;
    public final static int FIELD_FORMAT=4;
    public final static int FIELD_FOLDER_ID = 5;
    public final static int FIELD_DOUBLE = 6;
    public final static int FIELD_BEGIN_TIME = 7;
    public final static int FIELD_END_TIME = 8;

    public final static int FIELD_BEGIN = 9;
    public final static int FIELD_END = 10;

    public final static int FIELD_LOCAL_BEGIN = 11;
    public final static int FIELD_LOCAL_END = 12;

    public final static int FIELD_DATA = 13;

    public final static RangeFilter.IndexedField RANGE_LONG_FIELD = new RangeFilter.IndexedField(FIELD_BEGIN, FIELD_END);
    public final static RangeFilter.IndexedField RANGE_INSTANT_FIELD = new RangeFilter.IndexedField(FIELD_BEGIN_TIME, FIELD_END_TIME);
    public final static RangeFilter.IndexedField RANGE_LOCAL_FIELD = new RangeFilter.IndexedField(FIELD_LOCAL_BEGIN, FIELD_LOCAL_END);

    public static class FormatConverter extends EnumConverter<FormatType> {

        public FormatConverter() {
            super(FormatType.class);
        }
    }

    public StoreFileReadable(long id) {
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

    public Boolean isSingle() {
        return getBoolean(FIELD_SINGLE);
    }

    public FormatType getFormat() {
        return get(FIELD_FORMAT);
    }

    public Long getFolderId() {
        return getLong(FIELD_FOLDER_ID);
    }

    public Double getDouble() {
        return get(FIELD_DOUBLE);
    }

    public Instant getBeginTime() {
        return getInstant(FIELD_BEGIN_TIME);
    }

    public Instant getEndTime() {
        return getInstant(FIELD_END_TIME);
    }

    public Long getBegin() {
        return getLong(FIELD_BEGIN);
    }

    public Long getEnd() {
        return getLong(FIELD_END);
    }

    public LocalDateTime getLocalBegin() {
        return get(FIELD_LOCAL_BEGIN);
    }

    public LocalDateTime getLocalEnd() {
        return get(FIELD_LOCAL_END);
    }

    public byte[] getData() {
        return get(FIELD_DATA);
    }
}
