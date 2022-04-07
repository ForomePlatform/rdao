package org.forome.database.schema;

import org.forome.database.utils.TypeConvert;

import java.util.Collection;

public class PrefixIndex extends BaseIndex {

    private final static byte[] INDEX_NAME_BYTES = TypeConvert.pack("prf");

    PrefixIndex(org.forome.database.anotation.PrefixIndex index, StructEntity parent) {
        super(buildIndexedFields(index.fields(), parent), parent);
    }

    public static String toString(Collection<String> indexedFields) {
        return PrefixIndex.class.getSimpleName() + ": " + indexedFields;
    }

    @Override
    public byte[] getIndexNameBytes() {
        return INDEX_NAME_BYTES;
    }
}
