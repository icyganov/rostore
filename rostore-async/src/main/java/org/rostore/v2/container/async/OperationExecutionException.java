package org.rostore.v2.container.async;

import java.util.concurrent.ExecutionException;

public class OperationExecutionException extends ExecutionException {
    public OperationExecutionException(final Throwable thr) {
        super("Postponed exception from transaction execution", thr);
    }
}
