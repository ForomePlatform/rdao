package org.forome.database.maintenance;

import org.forome.database.domainobject.DomainObject;
import org.forome.database.domainobject.filter.IntervalFilter;
import org.forome.database.domainobject.filter.RangeFilter;
import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.exception.ColumnFamilyNotFoundException;
import org.forome.database.provider.DBIterator;
import org.forome.database.provider.DBTransaction;
import org.forome.database.provider.KeyPattern;
import org.forome.database.provider.KeyValue;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.database.domainobject.filter.PrefixFilter;
import org.forome.database.exception.DatabaseException;
import org.forome.database.exception.InconsistentDatabaseException;
import org.forome.database.schema.BaseIndex;
import org.forome.database.schema.HashIndex;
import org.forome.database.schema.Schema;
import org.forome.database.schema.StructEntity;
import org.forome.domain.ExchangeFolderReadable;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.forome.domain.type.FormatType;
import org.forome.database.domainobject.DomainDataTest;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;


public class DomainServiceTest extends DomainDataTest {

    @FunctionalInterface
    private interface Producer {

        void accept() throws DatabaseException;
    }

    @Before
    public void init() throws Exception {
        super.init();
        SchemaService.install(new HashSet<Class<? extends DomainObject>>() {{
            add(StoreFileReadable.class);
            add(ExchangeFolderReadable.class);
        }}, rocksDBProvider);
    }

    private Schema ensureSchema() throws DatabaseException {
        return Schema.read(rocksDBProvider);
    }

    @Test
    public void createAll() throws Exception {
        Schema schema = Schema.read(rocksDBProvider);
        schema.dropTable("StoreFile", "org.forome.store");
        schema.dropTable("ExchangeFolder", "org.forome.exchange");
        Schema.resolve(ExchangeFolderReadable.class);
        Schema.resolve(StoreFileReadable.class);
        testNotWorking();
        schema.createTable(Schema.getEntity(ExchangeFolderReadable.class));
        schema.createTable(Schema.getEntity(StoreFileReadable.class));
        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(StoreFileReadable.class))
                .execute();

        testWorking();
    }

    @Test
    public void createPartial() throws Exception {
        Schema schema = ensureSchema();
        StructEntity entity = Schema.getEntity(StoreFileReadable.class);

        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();
        rocksDBProvider.dropColumnFamily(entity.getColumnFamily());
        testNotWorking();
    }

    @Test
    public void createIndexAndIndexingDataAfterDropIndex() throws Exception {
        Schema schema = ensureSchema();
        StructEntity entity = Schema.getEntity(StoreFileReadable.class);
        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();

        domainObjectSource.executeTransactional(transaction -> {
            for (long i = 1; i < 100; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName("Test");
                obj.setSize(i);
                obj.setBegin(i);
                obj.setEnd(i+10);
                transaction.save(obj);
            }
        });
        rocksDBProvider.dropColumnFamily(entity.getIndexColumnFamily());
        Assertions.assertThatThrownBy(() -> {
            try (IteratorEntity ignored = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, 10L))) {
            }
        }).isExactlyInstanceOf(ColumnFamilyNotFoundException.class);
        Assertions.assertThatThrownBy(() -> {
            try (IteratorEntity ignored = domainObjectSource.find(StoreFileReadable.class, new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME,"tes"))) {
            }
        }).isExactlyInstanceOf(ColumnFamilyNotFoundException.class);
        Assertions.assertThatThrownBy(() -> {
            try (IteratorEntity ignored = domainObjectSource.find(StoreFileReadable.class, new IntervalFilter(StoreFileReadable.FIELD_SIZE, 35L, 50L))) {
            }
        }).isExactlyInstanceOf(ColumnFamilyNotFoundException.class);
        Assertions.assertThatThrownBy(() -> {
            try (IteratorEntity ignored = domainObjectSource.find(StoreFileReadable.class,
                    new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN, StoreFileReadable.FIELD_END), 35L, 50L))) {
            }
        }).isExactlyInstanceOf(ColumnFamilyNotFoundException.class);
    }

    @Test
    public void createIndexAndIndexingDataAfterDropEachIndex() throws Exception {
        Schema schema = ensureSchema();
        StructEntity entity = Schema.getEntity(StoreFileReadable.class);
        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();

        domainObjectSource.executeTransactional(transaction -> {
            for (long i = 1; i < 100; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName("Test");
                obj.setSize(i);
                obj.setBegin(i);
                obj.setEnd(i+10);
                transaction.save(obj);
            }
        });
        for (HashIndex hashIndex : entity.getHashIndexes()) {
            schema.dropIndex(hashIndex, entity.getName(), entity.getNamespace());
        }
        try (IteratorEntity ignored = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, 10L))) {
        }
    }

    @Test
    public void validateUnknownColumnFamily() {
        Schema schema = ensureSchema();
        rocksDBProvider.createColumnFamily("org.forome.store.StoreFile.some_prefix");
        Assertions.assertThatExceptionOfType(InconsistentDatabaseException.class).isThrownBy(() -> new DomainService(rocksDBProvider, schema)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(StoreFileReadable.class))
                .execute());
    }

//    @Test
//    public void remove() throws Exception{
//        Schema schema = ensureSchema();
//        Assertions.assertThat(schema.getDbSchema().getTables()).hasSize(2);
//
//        new DomainService(rocksDBProvider, schema)
//                .setChangeMode(ChangeMode.REMOVAL)
//                .setDomain(Schema.getEntity(StoreFileReadable.class))
//                .execute();
//        new DomainService(rocksDBProvider, schema)
//                .setChangeMode(ChangeMode.REMOVAL)
//                .setDomain(Schema.getEntity(ExchangeFolderReadable.class))
//                .execute();
//
//        Assert.assertArrayEquals(new String[] { Schema.SERVICE_COLUMN_FAMILY}, rocksDBProvider.getColumnFamilies());
//        Assertions.assertThat(schema.getDbSchema().getTables()).isEmpty();
//    }

//    @Test
//    public void removeAndValidate() throws Exception{
//        Schema schema = ensureSchema();
//
//        new DomainService(rocksDBProvider, schema)
//                .setChangeMode(ChangeMode.REMOVAL)
//                .setValidationMode(true)
//                .setDomain(Schema.getEntity(ExchangeFolderReadable.class))
//                .execute();
//        new DomainService(rocksDBProvider, schema)
//                .setChangeMode(ChangeMode.REMOVAL)
//                .setValidationMode(true)
//                .setDomain(Schema.getEntity(StoreFileReadable.class))
//                .execute();
//
//        Assert.assertArrayEquals(new String[] { Schema.SERVICE_COLUMN_FAMILY}, rocksDBProvider.getColumnFamilies());
//        Schema afterSchema = Schema.read(rocksDBProvider);
//        Assertions.assertThat(schema.getDbSchema().getTables()).isEmpty();
//        Assertions.assertThat(afterSchema.getDbSchema().getTables()).isEmpty();
//    }

    private void testNotWorking() throws Exception {
        try {
            testWorking();
            Assert.fail();
        } catch (DatabaseException ignoring) {
            Assert.assertTrue(true);
        }
    }

    private void testWorking() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Test");
            obj.setSize(100);
            obj.setFormat(FormatType.B);
            obj.setSingle(false);
            obj.setContentType("content");
            transaction.save(obj);
        });
    }

    private void removeIndex(BaseIndex index) {
        try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
            try (DBIterator it = domainObjectSource.createIterator(index.columnFamily)) {
                for (KeyValue kv = it.seek(new KeyPattern(index.attendant)); kv != null ; kv = it.next()) {
                    transaction.delete(index.columnFamily, kv.getKey());
                }
            }
            transaction.commit();
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkIndexExist(StructEntity entity, Producer before, Schema schema) throws Exception {
        before.accept();
        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, 10L))) {
            Assert.assertNotNull(iter.next());
        }

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class, new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME,"tes"))) {
            Assert.assertNotNull(iter.next());
        }

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class, new IntervalFilter(StoreFileReadable.FIELD_SIZE, 35L, 50L))) {
            long expectedId = 35;
            while (iter.hasNext()) {
                DomainObject st = iter.next();
                Assert.assertEquals(expectedId, st.getId());
                expectedId++;
            }
            Assert.assertEquals(50L, expectedId-1);
        }

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class,
                new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN, StoreFileReadable.FIELD_END), 35L, 50L))) {
            long expectedId = 35-9;
            while (iter.hasNext()) {
                DomainObject st = iter.next();
                Assert.assertEquals(expectedId, st.getId());
                expectedId++;
            }
            Assert.assertEquals(50L, expectedId);
        }
    }
}
