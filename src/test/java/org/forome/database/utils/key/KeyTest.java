package org.forome.database.utils.key;

import org.forome.database.utils.TypeConvert;
import org.forome.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.forome.database.schema.BaseIndex.ATTENDANT_BYTE_SIZE;

/**
 * Created by kris on 24.05.17.
 */
public class KeyTest {

    @Test
    public void testBeginningKey() {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());

            FieldKey key = new FieldKey(id);

            byte[] sKey = key.pack();

            Assert.assertEquals(id, FieldKey.unpackId(sKey));
            Assert.assertTrue(FieldKey.unpackBeginningObject(sKey));
        }
    }

    @Test
    public void testFieldKey() {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());
            String fieldName = UUID.randomUUID().toString().toLowerCase().replace("-", "");
            byte[] fieldNameBytes = TypeConvert.pack(fieldName);

            FieldKey key = new FieldKey(id, fieldNameBytes);

            byte[] sKey = key.pack();

            Assert.assertEquals(id, FieldKey.unpackId(sKey));
            Assert.assertEquals(fieldName, FieldKey.unpackFieldName(sKey));
            Assert.assertFalse(FieldKey.unpackBeginningObject(sKey));
        }
    }


    @Test
    public void testIndexKey() {
        for (int i = 0; i < 1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());
            long[] values = { Math.abs(RandomUtil.random.nextLong()), Math.abs(RandomUtil.random.nextLong())};
            byte[] attendant = new byte[ATTENDANT_BYTE_SIZE];
            KeyUtils.putAttendantBytes(attendant, "tst".getBytes(), TypeConvert.packCRC32(String.valueOf(RandomUtil.random.nextLong())));

            HashIndexKey key = new HashIndexKey(id, attendant, values);

            byte[] sKey = key.pack();

            HashIndexKey checkKey = HashIndexKey.unpack(sKey);
            Assert.assertEquals(id, checkKey.getId());
            Assert.assertArrayEquals(values, checkKey.getFieldValues());
            Assert.assertArrayEquals(attendant, checkKey.getAttendant());
        }
    }
}
