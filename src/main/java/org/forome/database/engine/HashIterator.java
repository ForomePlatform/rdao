package org.forome.database.engine;

import org.forome.database.domainobject.filter.HashFilter;
import org.forome.database.exception.DatabaseException;
import org.forome.database.exception.IllegalTypeException;
import org.forome.database.provider.DBDataReader;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.dbstruct.DBField;
import org.forome.database.schema.dbstruct.DBHashIndex;
import org.forome.database.schema.dbstruct.DBTable;
import org.forome.database.Record;
import org.forome.database.utils.HashIndexUtils;
import org.forome.database.utils.key.HashIndexKey;

import java.util.HashMap;
import java.util.Map;

public class HashIterator extends BaseIndexRecordIterator {

    private final Map<DBField, Object> filterFieldsValue = new HashMap<>();
    private KeyValue indexKeyValue;

    public HashIterator(DBTable table, HashFilter filter, DBDataReader dataReader) {
        super(table, dataReader);
        this.indexKeyValue = seekByFilter(table, filter);

        nextImpl();
    }

    @Override
    protected void nextImpl() throws DatabaseException {
        while (indexKeyValue != null) {
            nextRecord = findRecord(HashIndexKey.unpackId(indexKeyValue.getKey()));
            indexKeyValue = indexIterator.next();
            if (nextRecord != null) {
                return;
            }
        }

        nextRecord = null;
        close();
    }

    @Override
    boolean checkFilter(Record record) throws DatabaseException {
        return filterFieldsValue.entrySet()
                .stream()
                .allMatch(fieldEntry -> HashIndexUtils.equals(fieldEntry.getKey().getType(), fieldEntry.getValue(), record.getValues()[fieldEntry.getKey().getId()]));
    }

    private KeyValue seekByFilter(DBTable table, HashFilter filter) {
        Map<Integer, Object> filters = filter.getValues();
        final DBHashIndex index = table.getIndex(filter);

        long[] values = new long[index.getFieldIds().length];
        for (int i = 0; i < index.getFieldIds().length; ++i) {
            DBField field = table.getField(index.getFieldIds()[i]);
            Object value = filters.get(field.getId());
            checkValueType(value, field);

            values[i] = HashIndexUtils.buildHash(field.getType(), value, null);
            if (HashIndexUtils.toLongCastable(field.getType())) {
                continue;
            }
            filterFieldsValue.put(field, value);
        }
        return indexIterator.seek(HashIndexKey.buildKeyPattern(index, values));
    }

    private void checkValueType(Object value, DBField field) {
        if (value != null && field.getType() != value.getClass()) {
            throw new IllegalTypeException(field.getType(), value.getClass());
        }
    }
}
