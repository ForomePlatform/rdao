package org.forome.database.domainobject;

import org.forome.domain.ExchangeFolderEditable;
import org.forome.domain.StoreFileEditable;
import org.forome.domain.type.FormatType;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Created by kris on 22.04.17.
 */
public class DomainObjectTest extends StoreFileDataTest {

    @Test
    public void serialize() throws Exception {
        StoreFileEditable storeFile = new StoreFileEditable(1);
        storeFile.setContentType("application/json");
        storeFile.setFileName("info.json");
        storeFile.setSize(1000L);
        storeFile.setFormat(FormatType.B);
        testSerialize(storeFile);

        ExchangeFolderEditable exchangeFolder = new ExchangeFolderEditable(1);
        exchangeFolder.setUuid("1111");
        exchangeFolder.setUserEmail("2222@sgsdfg.com");
        exchangeFolder.setSyncDate(null);
        exchangeFolder.setSyncState("555");
        exchangeFolder.setParentId(10L);
        testSerialize(exchangeFolder);
    }

    private static void testSerialize(Serializable object) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(object);
                baos.toByteArray();
            }
        }
    }
}
