package org.forome.database.engine;

import org.forome.database.exception.DatabaseException;
import org.forome.database.exception.UnexpectedEndObjectException;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.KeyPattern;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.dbstruct.DBField;
import org.forome.database.schema.dbstruct.DBTable;
import org.forome.database.utils.key.FieldKey;
import org.forome.database.Record;
import org.forome.database.RecordIterator;
import org.forome.database.utils.TypeConvert;

import java.util.List;
import java.util.NoSuchElementException;

public abstract class BaseRecordIterator implements RecordIterator {

    public static class NextState {

        private long nextId;

        private NextState(long recordId) {
            this.nextId = recordId;
        }

        public long getNextId() {
            return nextId;
        }

        public boolean isEmpty() {
            return nextId == -1;
        }

        public void reset() {
            nextId = -1;
        }
    }

    protected Record nextRecord(DBTable table, NextState state, DBIterator dbIterator) throws DatabaseException {
        if (state.isEmpty()) {
            throw new NoSuchElementException();
        }
        return readRecord(table, state, dbIterator);
    }

    public static NextState seek(KeyPattern pattern, DBIterator iterator) throws DatabaseException {
        KeyValue keyValue = iterator.seek(pattern);
        if (keyValue == null) {
            return new NextState(-1);
        }

        if (!FieldKey.unpackBeginningObject(keyValue.getKey())) {
            return new NextState(-1);
        }

        long objId = FieldKey.unpackId(keyValue.getKey());
        return new NextState(objId);
    }

    public Record seekRecord(DBTable table, DBIterator iterator, KeyPattern pattern) throws DatabaseException {
        KeyValue keyValue = iterator.seek(pattern);
        if (keyValue == null) {
            return null;
        }

        if (!FieldKey.unpackBeginningObject(keyValue.getKey())) {
            return null;
        }

        return readRecord(table, FieldKey.unpackId(keyValue.getKey()), iterator);
    }

    private Record readRecord(DBTable table, NextState state, DBIterator iterator) throws DatabaseException {
        long recordId = state.nextId;
        List<DBField> fields = table.getSortedFields();
        Object[] values = new Object[fields.size()];
        KeyValue keyValue;
        while ((keyValue = iterator.next()) != null) {
            long id = FieldKey.unpackId(keyValue.getKey());
            if (id != recordId) {
                if (!FieldKey.unpackBeginningObject(keyValue.getKey())) {
                    throw new UnexpectedEndObjectException(recordId, id, FieldKey.unpackFieldName(keyValue.getKey()));
                }
                state.nextId = id;
                return new Record(recordId, values);
            }
            DBField field = table.getField(FieldKey.unpackFieldName(keyValue.getKey()));
            values[field.getId()] = TypeConvert.unpack(field.getType(), keyValue.getValue(), null);
        }
        state.nextId = -1;
        return new Record(recordId, values);
    }

    private Record readRecord(DBTable table, long recordId, DBIterator iterator) throws DatabaseException {
        List<DBField> fields = table.getSortedFields();
        Object[] values = new Object[fields.size()];
        KeyValue keyValue;
        while ((keyValue = iterator.next()) != null) {
            long id = FieldKey.unpackId(keyValue.getKey());
            if (id != recordId) {
                if (!FieldKey.unpackBeginningObject(keyValue.getKey())) {
                    throw new UnexpectedEndObjectException(recordId, id, FieldKey.unpackFieldName(keyValue.getKey()));
                }
                return new Record(recordId, values);
            }
            DBField field = table.getField(FieldKey.unpackFieldName(keyValue.getKey()));
            values[field.getId()] = TypeConvert.unpack(field.getType(), keyValue.getValue(), null);
        }
        return new Record(recordId, values);
    }
}
