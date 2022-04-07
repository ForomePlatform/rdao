package org.forome.database.utils.key;

import org.forome.database.schema.BaseIndex;

abstract class IndexKey extends Key {

    final byte[] attendant;

    IndexKey(long id, byte[] attendant) {
        super(id);
        checkAttendant(attendant);
        this.attendant = attendant;
    }

    private static void checkAttendant(final byte[] attendant) {
        if(attendant == null || attendant.length != BaseIndex.ATTENDANT_BYTE_SIZE) {
            throw new IllegalArgumentException();
        }
    }
}