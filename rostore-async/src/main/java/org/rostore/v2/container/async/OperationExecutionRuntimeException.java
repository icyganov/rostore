package org.rostore.v2.container.async;

public class OperationExecutionRuntimeException extends RuntimeException {

    public Throwable getRootCause() {
        return super.getCause().getCause();
    }

    public OperationExecutionRuntimeException(final OperationExecutionException o) {
        super(o);
    }
}
