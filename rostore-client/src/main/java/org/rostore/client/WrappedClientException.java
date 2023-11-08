package org.rostore.client;

public class WrappedClientException extends RuntimeException {
    public WrappedClientException(final String message, final Exception cause) {
        super(message, cause);
    }
}
