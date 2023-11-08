package org.rostore.entity;

public class RoStoreException extends RuntimeException {
    public RoStoreException(final String message) {
        super(message);
    }
    public RoStoreException(final String message, Throwable throwable) {
        super(message, throwable);
    }
}
