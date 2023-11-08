package org.rostore.entity;

public class QuotaExceededException extends RoStoreException {
    public QuotaExceededException(final String message) {
        super(message);
    }

    public QuotaExceededException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
