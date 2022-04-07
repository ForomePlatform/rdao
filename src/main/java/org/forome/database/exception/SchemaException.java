package org.forome.database.exception;

public class SchemaException extends RuntimeException {

    public SchemaException(Throwable cause) {
        super(cause);
    }

    public SchemaException(String message) {
        super(message);
    }
}
