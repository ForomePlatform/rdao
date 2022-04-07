package org.forome.database.domainobject.index;

import org.forome.database.domainobject.iterator.IteratorEntity;
import org.forome.database.domainobject.filter.HashFilter;
import org.forome.domain.ExchangeFolderEditable;
import org.forome.domain.ExchangeFolderReadable;
import org.forome.database.domainobject.ExchangeFolderDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class MultiIndexCreateTest extends ExchangeFolderDataTest {

    @Test
    public void run() throws Exception {
        String uuid = "AQMkAGYzOGZhMGRlLTk0ZmQtNGU4Mi05YzMyLWU1YmMyODgAMzA1MzkALgAAA2q7G9o/e25DjV2GPrKtaxsBAOVhxnfq2u5Gj3QIHLYcQRoAAAIBDQAAAA==";
        String userEmail = "test1@infomaximum.onmicrosoft.com";

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
                ExchangeFolderEditable exchangeFolder = transaction.create(ExchangeFolderEditable.class);
                exchangeFolder.setUuid(uuid);
                exchangeFolder.setUserEmail(userEmail);
                transaction.save(exchangeFolder);
        });

        //Ищем объект
         try (IteratorEntity<ExchangeFolderReadable> i = domainObjectSource.find(ExchangeFolderReadable.class, new HashFilter(ExchangeFolderReadable.FIELD_UUID, uuid)
              .appendField(ExchangeFolderReadable.FIELD_USER_EMAIL, userEmail))) {
             ExchangeFolderReadable exchangeFolder = i.next();
             Assert.assertNotNull(exchangeFolder);
             Assert.assertEquals(uuid, exchangeFolder.getUuid());
             Assert.assertEquals(userEmail, exchangeFolder.getUserEmail());
         }
    }
}
