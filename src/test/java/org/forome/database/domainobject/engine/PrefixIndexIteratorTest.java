package org.forome.database.domainobject.engine;

import org.forome.database.domainobject.StoreFileDataTest;
import org.forome.database.domainobject.filter.PrefixFilter;
import org.forome.database.utils.PrefixIndexUtils;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.StoreFileReadable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrefixIndexIteratorTest extends StoreFileDataTest {

    @Test
    public void findByOneField() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет всем");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("ПРИВЕТ ВСЕМ info.com");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("всем");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("прИВет всЕм .dor");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Александр Александров");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Реализация услуг акт Бухгалтерия предприятия, редакция рукпыва р р");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Иван Васильев");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Василий Иванов");
            transaction.save(obj);
        });

        final PrefixFilter filter = new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "");

        filter.setFieldValue("ghbdtn");
        assertFind(filter);

        filter.setFieldValue("привет");
        assertFind(filter, 1, 2 ,3, 5);

        filter.setFieldValue("вс");
        assertFind(filter, 1, 3, 4, 5);

        filter.setFieldValue("вс com");
        assertFind(filter, 3);

        filter.setFieldValue(".");
        assertFind(filter, 5);

        filter.setFieldValue("прив info");
        assertFind(filter, 3);

        filter.setFieldValue("алекс");
        assertFind(filter, 6);

        filter.setFieldValue("алекс алекс");
        assertFind(filter, 6);

        filter.setFieldValue("Р р");
        assertFind(filter, 7);

        filter.setFieldValue("Р");
        assertFind(filter, 7);

        filter.setFieldValue("Ре");
        assertFind(filter, 7);

        filter.setFieldValue("вас");
        assertFind(filter, 8, 9);
    }

    @Test
    public void findByTwoField() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет всем");
            obj.setContentType("test1");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет");
            obj.setContentType("test2 test21");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("ПРИВЕТ ВСЕМ info.com");
            obj.setContentType("test3 3test");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("всем");
            obj.setContentType("4test");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("прИВет всЕм .dor");
            obj.setContentType("5test rest");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Александр");
            obj.setContentType("Александров");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Реализация услуг акт Бухгалтерия ");
            obj.setContentType("предприятия, редакция рукпыва р р");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Александр");
            obj.setContentType("Иванов");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Андрей ");
            obj.setContentType("Александров");
            transaction.save(obj);
        });

        final PrefixFilter filter = new PrefixFilter(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_CONTENT_TYPE), "");

        filter.setFieldValue("ghbdtn");
        assertFind(filter);

        filter.setFieldValue("привет test21");
        assertFind(filter, 2);

        filter.setFieldValue("вс test");
        assertFind(filter, 1, 3);

        filter.setFieldValue("test вс com");
        assertFind(filter, 3);

        filter.setFieldValue("4test");
        assertFind(filter, 4);

        filter.setFieldValue("5te res");
        assertFind(filter, 5);

        filter.setFieldValue("алекс");
        assertFind(filter, 6, 8, 9);

        filter.setFieldValue("алекс алекс");
        assertFind(filter, 6);

        filter.setFieldValue("Р");
        assertFind(filter, 7);

        filter.setFieldValue("Ре");
        assertFind(filter, 7);
    }

    @Test
    public void findAmongBlocks() throws Exception {
        final int idCount = 3 * PrefixIndexUtils.PREFERRED_MAX_ID_COUNT_PER_BLOCK + 200;
        final List<Long> expectedIds = new ArrayList<>(idCount);
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < idCount; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName("ПРИВЕТ ВСЕМ info.com");
                transaction.save(obj);

                expectedIds.add(obj.getId());
            }
        });

        final PrefixFilter filter = new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "всем");

        assertFind(filter, expectedIds);
    }

//    @Test
//    public void removeAndFind() throws Exception {
//        domainObjectSource.executeTransactional(transaction -> {
//            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
//            obj.setFileName("hello");
//            transaction.save(obj);
//
//            obj = transaction.create(StoreFileEditable.class);
//            obj.setFileName("by");
//            transaction.save(obj);
//
//            obj = transaction.create(StoreFileEditable.class);
//            obj.setFileName("hello");
//            transaction.save(obj);
//        });
//
//        domainObjectSource.executeTransactional(transaction -> {
//            transaction.remove(transaction.get(StoreFileEditable.class, 1));
//            transaction.remove(transaction.get(StoreFileEditable.class, 2));
//
//            testFind(transaction, new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "by"));
//            testFind(transaction, new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "hel"), 3);
//        });
//
//        testFind(domainObjectSource, new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "by"));
//        testFind(domainObjectSource, new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME, "hel"), 3);
//    }
}
