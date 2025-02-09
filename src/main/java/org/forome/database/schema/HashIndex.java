package org.forome.database.schema;

import org.forome.database.utils.TypeConvert;

import java.util.Collection;
import java.util.Collections;

public class HashIndex extends BaseIndex {

    private final static byte[] INDEX_NAME_BYTES = TypeConvert.pack("hsh");

    HashIndex(org.forome.database.anotation.HashIndex index, StructEntity parent) {
        super(buildIndexedFields(index.fields(), parent), parent);
    }

    public HashIndex(Field field, StructEntity parent) {
        super(Collections.singletonList(field), parent);
    }

    public static String toString(Collection<String> indexedFields) {
        return HashIndex.class.getSimpleName() + ": " + indexedFields;
    }

    @Override
    public byte[] getIndexNameBytes() {
        return INDEX_NAME_BYTES;
    }
}
