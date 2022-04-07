package org.forome.database.domainobject.proxy;

import org.forome.database.domainobject.Transaction;
import org.forome.domain.proxy.ProxyStoreFileEditable;
import org.forome.domain.proxy.ProxyStoreFileReadable;
import org.forome.database.domainobject.DomainDataTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class EditProxyDomainObjectTest extends DomainDataTest {

    @Before
    public void init() throws Exception {
        super.init();

        createDomain(ProxyStoreFileReadable.class);
    }

    @Test
    public void run() throws Exception {
        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(ProxyStoreFileReadable.class, 1L));

        String fileName1 = "info1.json";
        String fileName2 = "info2.json";
        String contentType = "application/json";
        long size = 1000L;

        //Добавляем объект
        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            ProxyStoreFileEditable storeFile = transaction.create(ProxyStoreFileEditable.class);
            storeFile.setFileName(fileName1);
            storeFile.setContentType(contentType);
            storeFile.setSize(size);
            storeFile.setSingle(false);
            transaction.save(storeFile);
            transaction.commit();
        }

        //Загружаем сохраненый объект
        ProxyStoreFileReadable storeFileCheckSave = domainObjectSource.get(ProxyStoreFileReadable.class, 1L);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(fileName1, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave.getContentType());
        Assert.assertEquals(size, storeFileCheckSave.getSize());
        Assert.assertFalse(storeFileCheckSave.isSingle());

        //Редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
            ProxyStoreFileEditable obj = domainObjectSource.get(ProxyStoreFileEditable.class, 1L);
            obj.setFileName(fileName2);
            obj.setSingle(true);
            transaction.save(obj);
        });

        //Загружаем отредактированный объект
        ProxyStoreFileReadable editFileCheckSave = domainObjectSource.get(ProxyStoreFileReadable.class, 1L);
        Assert.assertNotNull(editFileCheckSave);
        Assert.assertEquals(fileName2, editFileCheckSave.getFileName());
        Assert.assertEquals(contentType, editFileCheckSave.getContentType());
        Assert.assertEquals(size, editFileCheckSave.getSize());
        Assert.assertTrue(editFileCheckSave.isSingle());

        //Повторно редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
            ProxyStoreFileEditable obj = domainObjectSource.get(ProxyStoreFileEditable.class, 1L);
            obj.setFileName(fileName1);
            obj.setSingle(false);
            transaction.save(obj);
        });

        ProxyStoreFileReadable storeFileCheckSave2 = domainObjectSource.get(ProxyStoreFileReadable.class, 1L);
        Assert.assertNotNull(storeFileCheckSave2);
        Assert.assertEquals(fileName1, storeFileCheckSave2.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave2.getContentType());
        Assert.assertEquals(size, storeFileCheckSave2.getSize());
        Assert.assertFalse(storeFileCheckSave2.isSingle());
    }
}
