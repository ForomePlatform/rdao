package org.forome.database.domainobject.index;

import org.forome.database.utils.HashIndexUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class HashIndexUtilsTest {

    @Test
    public void buildHashOfString() {
        Assertions.assertThat(HashIndexUtils.buildHash(String.class, null, null)).isEqualTo(0L);
        Assertions.assertThat(HashIndexUtils.buildHash(String.class, "", null)).isEqualTo(0L);

        Assertions.assertThat(HashIndexUtils.buildHash(String.class, "english text", null)).isEqualTo(2171775918L);
        Assertions.assertThat(HashIndexUtils.buildHash(String.class, "english TEXT", null)).isEqualTo(2171775918L);

        Assertions.assertThat(HashIndexUtils.buildHash(String.class, "русский текст", null)).isEqualTo(4148994842L);
        Assertions.assertThat(HashIndexUtils.buildHash(String.class, "русский ТЕКСТ", null)).isEqualTo(4148994842L);

        Assertions.assertThat(HashIndexUtils.buildHash(String.class, "mixed текст", null)).isEqualTo(480766946L);

        Assertions.assertThat(HashIndexUtils.buildHash(String.class, "mixed текс", null)).isEqualTo(2958377010L);

        Assertions.assertThat(HashIndexUtils.buildHash(String.class, "mixeed текс", null)).isEqualTo(2549465030L);
    }
}
