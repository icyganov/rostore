package org.rostore.v2.container.async;

public class AsyncException extends RuntimeException {

    public AsyncException(final Throwable thr) {
        super("Async exception has happened", thr);
    }
}
