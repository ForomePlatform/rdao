package org.forome.database.utils.key;

import org.forome.database.schema.BaseIntervalIndex;
import org.forome.database.schema.dbstruct.DBIntervalIndex;
import org.forome.database.utils.IntervalIndexUtils;
import org.forome.database.utils.TypeConvert;

import java.nio.ByteBuffer;

public class IntervalIndexKey extends BaseIntervalIndexKey {

    public IntervalIndexKey(long id, final long[] hashedValues, BaseIntervalIndex index) {
        super(id, hashedValues, index.attendant);
    }

    public IntervalIndexKey(long id, final long[] hashedValues, DBIntervalIndex index) {
        super(id, hashedValues, index.getAttendant());
    }

    public void setIndexedValue(Object value) {
        indexedValue = IntervalIndexUtils.castToLong(value);
    }

    @Override
    public byte[] pack() {
        ByteBuffer buffer = TypeConvert.allocateBuffer(attendant.length + ID_BYTE_SIZE * (hashedValues.length + 2) + Byte.BYTES);
        fillBuffer(attendant, hashedValues, indexedValue, buffer);
        buffer.putLong(getId());
        return buffer.array();
    }

    public static long unpackIndexedValue(final byte[] src) {
        return TypeConvert.unpackLong(src, src.length - 2 * ID_BYTE_SIZE);
    }
}
