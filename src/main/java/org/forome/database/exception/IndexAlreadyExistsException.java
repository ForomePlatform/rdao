package org.forome.database.exception;

import org.forome.database.schema.BaseIndex;
import org.forome.database.schema.dbstruct.DBIndex;

public class IndexAlreadyExistsException extends SchemaException {

    public <T extends BaseIndex> IndexAlreadyExistsException(T index) {
        super("Index already exists, " + index.toString());
    }

    public <T extends DBIndex> IndexAlreadyExistsException(T index) {
        super("Index already exists, " + index.toString());
    }
}
