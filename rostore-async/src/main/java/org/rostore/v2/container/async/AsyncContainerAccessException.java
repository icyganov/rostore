package org.rostore.v2.container.async;

import org.rostore.entity.RoStoreException;

public class AsyncContainerAccessException extends RoStoreException {

    public AsyncContainerAccessException(String message) {
        super(message);
    }

    public AsyncContainerAccessException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
