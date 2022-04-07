package org.forome.database.domainobject.index;

import org.forome.database.domainobject.Value;
import org.forome.database.utils.PrefixIndexUtils;
import org.forome.database.utils.TypeConvert;
import org.forome.database.utils.key.Key;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.nio.LongBuffer;
import java.util.*;

public class PrefixIndexUtilsTest {

    @Test
    public void diffIndexedLexemes() {
        assertEquals(
                buildValues("test01 test02 test03", "test12 test12 test13"),
                buildValues(),
                Collections.emptySet(),
                Collections.emptySet()
        );

        assertEquals(buildValues("test01 test02 test03", "test12 test12 test13"),
                buildValues("test01 test02 test03", "test12 test12 test13"),
                Collections.emptySet(),
                Collections.emptySet()
        );

        assertEquals(
                buildValues("test01 test02 test03", "test12 test12 test13"),
                buildValues("test12 test12 test13", "test01 test02 test03"),
                Collections.emptySet(),
                Collections.emptySet()
        );

        assertEquals(
                buildValues("test01 test02 test03", "test12 test12 test13"),
                buildValues("test01 test02 test", "test13 test"),
                new HashSet<>(Arrays.asList("test03", "test12")),
                Collections.emptySet()
        );

        assertEquals(
                buildValues("test01 test02 test03", "test12 test12 test13"),
                buildValues(null, null),
                new HashSet<>(Arrays.asList("test01", "test02", "test03", "test12", "test13")),
                Collections.emptySet()
        );

        assertEquals(buildValues("test01 test02 test03", "test12 test12 test13"),
                buildValues("test01 test02 test03", null),
                new HashSet<>(Arrays.asList("test12", "test13")),
                Collections.emptySet()
        );
    }

    private static void assertEquals(Value<Serializable>[] prevValues, Value<Serializable>[] newValues,
                                     Set<String> expectedDeletingLexemes, Set<String> expectedInsertingLexemes) {
        final Set<String> actualDeletingLexemes = new HashSet<>();
        final Set<String> actualInsertingLexemes = new HashSet<>();

        PrefixIndexUtils.diffIndexedLexemes(Arrays.asList(0, 1), prevValues, newValues, actualDeletingLexemes, actualInsertingLexemes, integer -> integer);
        Assert.assertEquals("Deleting lexemes", expectedDeletingLexemes, actualDeletingLexemes);
        Assert.assertEquals("Inserting lexemes", expectedInsertingLexemes, actualInsertingLexemes);
    }

    @Test
    public void splitIndexedWhitespaceBeginingText() {
        final String text = " Привет comparator Медвед infom.COM  com   cOmpa comPArat test...2d sop@ru \n \r";

        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        PrefixIndexUtils.splitIndexingTextIntoLexemes(text, lexemes);
        Assert.assertArrayEquals(
                new String[] {"привет", "медвед", "test...2d", "sop@ru", "ru", "infom.com", "comparator", "2d"},
                lexemes.toArray()
        );
    }

    @Test
    public void splitIndexedText() {
        final String text = "Привет Медвед infom.COM  i com  ...test...2d sop@ru м м мед медв";

        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        PrefixIndexUtils.splitIndexingTextIntoLexemes(text, lexemes);
        Assert.assertArrayEquals(
                new String[] {"привет", "медвед", "test...2d", "sop@ru", "ru", "infom.com", "com", "2d", "...test...2d"},
                lexemes.toArray()
        );
    }

    @Test
    public void appendId() {
        byte[] bytes = TypeConvert.pack(100L);
        bytes = PrefixIndexUtils.appendId(200, bytes);
        bytes = PrefixIndexUtils.appendId(20, bytes);
        bytes = PrefixIndexUtils.appendId(30, bytes);
        bytes = PrefixIndexUtils.appendId(400, bytes);

        assertArrayEquals(new long[] {20, 30, 100, 200, 400}, bytes);
    }

    @Test
    public void removeId() {
        byte[] bytes = TypeConvert.pack(100L);
        bytes = PrefixIndexUtils.appendId(200, bytes);
        bytes = PrefixIndexUtils.appendId(30, bytes);
        bytes = PrefixIndexUtils.appendId(400, bytes);

        byte[] newBytes = PrefixIndexUtils.removeId(10, bytes);
        Assert.assertNull(newBytes);

        newBytes = PrefixIndexUtils.removeId(200, bytes);
        assertArrayEquals(new long[] {30, 100, 400 }, newBytes);

        newBytes = PrefixIndexUtils.removeId(400, newBytes);
        assertArrayEquals(new long[] {30, 100 }, newBytes);

        newBytes = PrefixIndexUtils.removeId(100, newBytes);
        assertArrayEquals(new long[] { 30 }, newBytes);

        newBytes = PrefixIndexUtils.removeId(30, newBytes);
        Assert.assertArrayEquals(new byte[0], newBytes);
    }

    @Test
    public void getIdCount() {
        byte[] bytes = TypeConvert.allocateBuffer(4 * Key.ID_BYTE_SIZE)
                .putLong(100)
                .putLong(200)
                .putLong(30)
                .putLong(400)
                .array();

        Assert.assertEquals(4, PrefixIndexUtils.getIdCount(bytes));
    }

    @Test
    public void splitSearchingTextIntoWords() {
        final String text = " Привет Медвед infom.COM COM  com  test...2d sop@ru \n \r";

        List<String> lexemes = PrefixIndexUtils.splitSearchingTextIntoWords(text);
        Assert.assertArrayEquals(Arrays.asList(
                "com",
                "com",
                "привет",
                "медвед",
                "sop@ru",
                "infom.com",
                "test...2d").toArray(), lexemes.toArray());
    }

    @Test
    public void contains() {
        final String[] texts = {" Привет Медвед infom.COM COM  com  test...2d sop@ru \n \r", "ru.yandex perl"};
        List<String> tempList = new ArrayList<>();

        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив мед"), texts, tempList));
        Assert.assertFalse(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив прив"), texts, tempList));
        Assert.assertFalse(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив мед мед мед мед мед мед мед мед мед мед"), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" прив \n мед "), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" tes co com sop@ "), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" ru "), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" com ru "), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" com ru 2d"), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" com ru yand"), texts, tempList));
    }

    private static void assertArrayEquals(long[] expected, byte[] actual) {
        LongBuffer buffer = TypeConvert.wrapBuffer(actual).asLongBuffer();
        Assert.assertEquals("Size", expected.length, buffer.limit());
        for (long val : expected) {
            Assert.assertEquals(val, buffer.get());
        }
    }

    private static Value<Serializable>[] buildValues(String... values) {
        Value<Serializable>[] result = new Value[values.length];
        for (int i = 0; i < values.length; ++i) {
            result[i] = Value.of(values[i]);
        }
        return result;
    }
}
