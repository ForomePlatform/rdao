package org.forome.domain.type;

import org.forome.database.utils.BaseEnum;

/**
 * Created by kris on 16.06.17.
 */
public enum FormatType implements BaseEnum {

    A(1),

    B(2);

    private final int value;

    FormatType(int value) {
        this.value = value;
    }

    @Override
    public int intValue() {
        return value;
    }
}
