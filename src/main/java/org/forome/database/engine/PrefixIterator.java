package org.forome.database.engine;

import org.forome.database.domainobject.filter.PrefixFilter;
import org.forome.database.exception.DatabaseException;
import org.forome.database.provider.DBDataReader;
import org.forome.database.provider.KeyPattern;
import org.forome.database.provider.KeyValue;
import org.forome.database.schema.dbstruct.DBPrefixIndex;
import org.forome.database.schema.dbstruct.DBTable;
import org.google.common.collect.Range;
import org.google.common.collect.RangeSet;
import org.google.common.collect.TreeRangeSet;
import org.forome.database.Record;
import org.forome.database.utils.PrefixIndexUtils;
import org.forome.database.utils.TypeConvert;
import org.forome.database.utils.key.PrefixIndexKey;

import java.nio.ByteBuffer;
import java.util.*;

public class PrefixIterator extends BaseIndexRecordIterator {

    private final DBPrefixIndex index;

    private List<String> searchingWords;
    private ByteBuffer loadingIds = null;
    private final RangeSet<Long> prevLoadedIds = TreeRangeSet.create();

    private List<String> tempList;

    public PrefixIterator(DBTable table, PrefixFilter filter, DBDataReader dataReader) {
        super(table, dataReader);

        this.index = table.getIndex(filter);
        this.searchingWords = PrefixIndexUtils.splitSearchingTextIntoWords(filter.getFieldValue());
        if (this.searchingWords.isEmpty()) {
            return;
        }

        KeyPattern indexKeyPattern = PrefixIndexKey.buildKeyPatternForFind(searchingWords.get(searchingWords.size() - 1), index);
        if (this.searchingWords.size() <= 1) {
            this.searchingWords = Collections.emptyList();
        }

        this.tempList = new ArrayList<>();

        KeyValue keyValue = indexIterator.seek(indexKeyPattern);
        this.loadingIds = keyValue != null ? TypeConvert.wrapBuffer(keyValue.getValue()) : null;

        nextImpl();
    }

    @Override
    protected void nextImpl() throws DatabaseException {
        while (loadingIds != null) {
            if (!loadingIds.hasRemaining()) {
                KeyValue keyValue = indexIterator.next();
                loadingIds = keyValue != null ? TypeConvert.wrapBuffer(keyValue.getValue()) : null;
                continue;
            }

            final long id = loadingIds.getLong();
            if (prevLoadedIds.contains(id)) {
                continue;
            }

            nextRecord = findRecord(id);
            if (nextRecord != null) {
                prevLoadedIds.add(Range.closedOpen(id, id + 1));
                return;
            }
        }

        nextRecord = null;
        close();
    }

    @Override
    boolean checkFilter(Record record) throws DatabaseException {
        String[] values = new String[index.getFieldIds().length];
        for (int i = 0; i < index.getFieldIds().length; ++i) {
            values[i] = record.getValues()[index.getFieldIds()[i]].toString();
        }
        return PrefixIndexUtils.contains(searchingWords, values, tempList);
    }
}
