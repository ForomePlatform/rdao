package org.forome.database.maintenance;

import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.DomainObjectSource;
import org.forome.database.domainobject.filter.EmptyFilter;
import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.exception.ForeignDependencyException;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.forome.database.exception.DatabaseException;
import org.forome.database.exception.InconsistentDatabaseException;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.DBProvider;
import org.forome.database.schema.Field;
import org.forome.database.schema.StructEntity;
import org.forome.database.schema.Schema;
import org.forome.database.utils.key.FieldKey;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DomainService {

    private final DBProvider dbProvider;

    private ChangeMode changeMode = ChangeMode.NONE;
    private boolean isValidationMode = false;

    private StructEntity domain;
    private final Schema dbSchema;
    private boolean existsData = false;

    public DomainService(DBProvider dbProvider, Schema dbSchema) {
        this.dbProvider = dbProvider;
        this.dbSchema = dbSchema;
    }

    public DomainService setChangeMode(ChangeMode value) {
        this.changeMode = value;
        return this;
    }

    public DomainService setValidationMode(boolean value) {
        this.isValidationMode = value;
        return this;
    }

    public DomainService setDomain(StructEntity value) {
        this.domain = value;
        return this;
    }

    public void execute() throws DatabaseException {
        final String dataColumnFamily = domain.getColumnFamily();

        existsData = isExistsColumnFamily(dataColumnFamily);

        if (changeMode == ChangeMode.REMOVAL) {
//            remove();
        }

        validate();
    }

    static void removeDomainColumnFamiliesFrom(Set<String> columnFamilies, final StructEntity domain) {
        columnFamilies.remove(domain.getColumnFamily());
        columnFamilies.remove(domain.getIndexColumnFamily());
    }

    //todo add it to remove module
    private void remove() throws DatabaseException {
        dbSchema.dropTable(domain.getName(), domain.getNamespace());
        for (String columnFamily : getColumnFamilies()) {
            dbProvider.dropColumnFamily(columnFamily);
        }
    }

    private void validate() throws DatabaseException {
        if (!isValidationMode) {
            return;
        }

        validateUnknownColumnFamilies();

        if (changeMode != ChangeMode.REMOVAL) {
            validateIntegrity();
        }
    }

    private Set<String> getColumnFamilies() throws DatabaseException {
        final String namespacePrefix = domain.getColumnFamily() + StructEntity.NAMESPACE_SEPARATOR;
        Set<String> result = Arrays.stream(dbProvider.getColumnFamilies())
                .filter(s -> s.startsWith(namespacePrefix))
                .collect(Collectors.toSet());
        result.add(domain.getColumnFamily());
        return result;
    }

    private void validateUnknownColumnFamilies() throws DatabaseException {
        Set<String> columnFamilies = getColumnFamilies();
        removeDomainColumnFamiliesFrom(columnFamilies, domain);
        if (!columnFamilies.isEmpty()) {
            throw new InconsistentDatabaseException(domain.getObjectClass() + " contains unknown column families " + String.join(", ", columnFamilies) + ".");
        }
    }

    private boolean isExistsColumnFamily(String columnFamily) throws DatabaseException {
        if (dbProvider.containsColumnFamily(columnFamily)) {
            return existsKeys(columnFamily);
        }
//
//        if (changeMode == ChangeMode.CREATION) {
//            dbProvider.createColumnFamily(columnFamily);
//        } else if (isValidationMode) {
//            throw new InconsistentDatabaseException("Column family " + columnFamily + " not found.");
//        }
        return false;
    }

    private boolean existsKeys(String columnFamily) throws DatabaseException {
        try (DBIterator i = dbProvider.createIterator(columnFamily)) {
            return i.seek(null) != null;
        }
    }

    private void validateIntegrity() throws DatabaseException {
        if (!existsData) {
            return;
        }

        List<Field> foreignFields = Arrays.stream(domain.getFields())
                .filter(Field::isForeign)
                .collect(Collectors.toList());

        if (foreignFields.isEmpty()) {
            return;
        }

        Set<Integer> fieldNames = foreignFields
                .stream()
                .map(Field::getNumber)
                .collect(Collectors.toSet());

        FieldKey fieldKey = new FieldKey(0);

        RangeSet<Long>[] processedIds = new RangeSet[domain.getFields().length];
        for (Field field : foreignFields) {
            processedIds[field.getNumber()] = TreeRangeSet.create();
        }

        DomainObjectSource domainObjectSource = new DomainObjectSource(dbProvider);
        try (IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(domain.getObjectClass(), EmptyFilter.INSTANCE, fieldNames)) {
            while (iter.hasNext()) {
                DomainObject obj = iter.next();

                for (Field field : foreignFields) {
                    Long value = obj.get(field.getNumber());
                    if (value == null) {
                        continue;
                    }

                    RangeSet<Long> processedId = processedIds[field.getNumber()];
                    if (processedId.contains(value)) {
                        continue;
                    }

                    fieldKey.setId(value);
                    if (dbProvider.getValue(field.getForeignDependency().getColumnFamily(), fieldKey.pack()) == null) {
                        throw new ForeignDependencyException(obj.getId(), domain.getObjectClass(), field, value);
                    }
                    processedId.add(Range.closedOpen(value, value + 1));
                }
            }
        }
    }
}
