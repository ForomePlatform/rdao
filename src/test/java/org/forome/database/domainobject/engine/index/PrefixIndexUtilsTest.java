package org.forome.database.domainobject.engine.index;

import org.forome.database.utils.PrefixIndexUtils;
import org.forome.database.utils.TypeConvert;
import org.forome.database.utils.key.Key;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.LongBuffer;
import java.util.*;

public class PrefixIndexUtilsTest {

    @Test
    public void diffIndexedLexemes() {
        assertEquals(
                new String[] {"test01 test02 test03", "test12 test12 test13"},
                new String[] {},
                Collections.emptySet(),
                Collections.emptySet()
        );

        assertEquals(
                new String[] {"test01 test02 test03", "test12 test12 test13"},
                new String[] {"test01 test02 test03", "test12 test12 test13"},
                Collections.emptySet(),
                Collections.emptySet()
        );

        assertEquals(
                new String[] {"test01 test02 test03", "test12 test12 test13"},
                new String[] {"test12 test12 test13", "test01 test02 test03"},
                Collections.emptySet(),
                Collections.emptySet()
        );

        assertEquals(
                new String[] {"test01 test02 test03", "test12 test12 test13"},
                new String[] {"test01 test02 test", "test13 test"},
                new HashSet<>(Arrays.asList("test03", "test12")),
                Collections.emptySet()
        );

        assertEquals(
                new String[] {"test01 test02 test03", "test12 test12 test13"},
                new String[] {null, null},
                new HashSet<>(Arrays.asList("test01", "test02", "test03", "test12", "test13")),
                Collections.emptySet()
        );

        assertEquals(
                new String[] {"test01 test02 test03", "test12 test12 test13"},
                new String[] {"test01 test02 test03", null},
                new HashSet<>(Arrays.asList("test12", "test13")),
                Collections.emptySet()
        );
    }

    private static void assertEquals(Object[] prevValues, Object[] newValues,
                                     Set<String> expectedDeletingLexemes, Set<String> expectedInsertingLexemes) {
        final Set<String> actualDeletingLexemes = new HashSet<>();
        final Set<String> actualInsertingLexemes = new HashSet<>();

        PrefixIndexUtils.diffIndexedLexemes(new int[] {0, 1}, prevValues, newValues, actualDeletingLexemes, actualInsertingLexemes);
        Assertions.assertThat(expectedDeletingLexemes)
                .as("Deleting lexemes")
                .containsAll(actualDeletingLexemes);
        Assertions.assertThat(expectedInsertingLexemes)
                .as("Inserting lexemes")
                .containsAll(actualInsertingLexemes);
    }

    @Test
    public void splitIndexedWhitespaceBeginingText() {
        final String text = " Привет comparator Медвед infom.COM  com   cOmpa comPArat test...2d sop@ru \n \r";

        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        PrefixIndexUtils.splitIndexingTextIntoLexemes(text, lexemes);
        Assertions.assertThat(lexemes.toArray())
                .containsOnly(new String[] {"привет", "медвед", "test...2d", "sop@ru", "ru", "infom.com", "comparator", "2d"});
    }

    @Test
    public void splitIndexedText() {
        final String text = "Привет Медвед infom.COM  i com  ...test...2d sop@ru м м мед медв";

        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        PrefixIndexUtils.splitIndexingTextIntoLexemes(text, lexemes);
        Assertions.assertThat(lexemes.toArray())
                .containsOnly(new String[] {"привет", "медвед", "test...2d", "sop@ru", "ru", "infom.com", "com", "2d", "...test...2d"});
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
        Assertions.assertThat(newBytes).isNull();

        newBytes = PrefixIndexUtils.removeId(200, bytes);
        assertArrayEquals(new long[] {30, 100, 400 }, newBytes);

        newBytes = PrefixIndexUtils.removeId(400, newBytes);
        assertArrayEquals(new long[] {30, 100 }, newBytes);

        newBytes = PrefixIndexUtils.removeId(100, newBytes);
        assertArrayEquals(new long[] { 30 }, newBytes);

        newBytes = PrefixIndexUtils.removeId(30, newBytes);
        Assertions.assertThat(new byte[0])
                .isEqualTo(newBytes);
    }

    @Test
    public void getIdCount() {
        byte[] bytes = TypeConvert.allocateBuffer(4 * Key.ID_BYTE_SIZE)
                .putLong(100)
                .putLong(200)
                .putLong(30)
                .putLong(400)
                .array();

        Assertions.assertThat(PrefixIndexUtils.getIdCount(bytes))
                .isEqualTo(4);
    }

    @Test
    public void splitSearchingTextIntoWords() {
        final String text = " Привет Медвед infom.COM COM  com  test...2d sop@ru \n \r";

        List<String> lexemes = PrefixIndexUtils.splitSearchingTextIntoWords(text);
        Assertions.assertThat(lexemes.toArray())
                .isEqualTo(Arrays.asList(
                        "com",
                        "com",
                        "привет",
                        "медвед",
                        "sop@ru",
                        "infom.com",
                        "test...2d").toArray());
    }

    @Test
    public void contains() {
        final String[] texts = {" Привет Медвед infom.COM COM  com  test...2d sop@ru \n \r", "ru.yandex perl"};
        List<String> tempList = new ArrayList<>();

        Assertions.assertThat(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив мед"), texts, tempList))
                .isTrue();
        Assertions.assertThat(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив прив"), texts, tempList))
                .isFalse();
        Assertions.assertThat(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив мед мед мед мед мед мед мед мед мед мед"), texts, tempList))
                .isFalse();
        Assertions.assertThat(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" прив \n мед "), texts, tempList))
                .isTrue();
        Assertions.assertThat(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" tes co com sop@ "), texts, tempList))
                .isTrue();
        Assertions.assertThat(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" ru "), texts, tempList))
                .isTrue();
        Assertions.assertThat(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" com ru "), texts, tempList))
                .isTrue();
        Assertions.assertThat(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" com ru 2d"), texts, tempList))
                .isTrue();
        Assertions.assertThat(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" com ru yand"), texts, tempList))
                .isTrue();
    }

    private static void assertArrayEquals(long[] expected, byte[] actual) {
        LongBuffer buffer = TypeConvert.wrapBuffer(actual).asLongBuffer();
        Assertions.assertThat(buffer.limit())
                .as("Size")
                .isEqualTo(expected.length);
        for (long val : expected) {
            Assertions.assertThat(val)
                    .isEqualTo(buffer.get());
        }
    }
}
