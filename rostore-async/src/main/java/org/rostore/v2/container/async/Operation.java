package org.rostore.v2.container.async;

import org.rostore.entity.RoStoreException;

import java.util.concurrent.*;
import java.util.function.Supplier;

public class Operation<R> implements Future<R> {

    private final int sessionId;
    private final OperationType operationType;
    private final OperationTarget operationTarget;
    private final Supplier<R> function;
    private R result;
    private Exception exception;
    private final CountDownLatch countDownLatch;
    private boolean done;

    // only available for value ops
    private final long valueId;
    // if no-body waits for the ops

    public String toString() {
        return "Operation " + operationTarget + " " + operationType + (valueId != -1 ? " valueId="+valueId : "");
    }

    public long getValueId() {
        if (valueId == -1) {
            throw new RoStoreException("Operation is not a value operation!");
        }
        return valueId;
    }

    public static <R> Operation<R> value(final int sessionId, final OperationType operationType, final long valueId, final Supplier<R> function) {
        return new Operation<>(sessionId, OperationTarget.VALUE, operationType, valueId, false, function);
    }

    public static Operation autonomousValue(final int sessionId, final OperationType operationType, final long valueId, final Runnable runnable) {
        return new Operation<>(sessionId, OperationTarget.VALUE, operationType, valueId, true, () -> {
            runnable.run();
            return true;
        });
    }

    public static <R> Operation<R> key(final int sessionId, final OperationType operationType, final Supplier<R> function) {
        return new Operation<>(sessionId, OperationTarget.KEY, operationType, -1, false, function);
    }

    public static Operation autonomousKey(final int sessionId, final OperationType operationType, final Runnable runnable) {
        return new Operation<>(sessionId, OperationTarget.KEY, operationType, -1, true, () -> {
            runnable.run();
            return true;
        });
    }

    private Operation(final int sessionId, final OperationTarget target, final OperationType operationType, final long valueId, final boolean autonomous, final Supplier<R> function) {
        this.sessionId = sessionId;
        this.operationType = operationType;
        this.function = function;
        countDownLatch = autonomous ? null : new CountDownLatch(1);
        done = false;
        this.valueId = valueId;
        this.operationTarget = target;
    }

    public int getSessionId() {
        return sessionId;
    }

    public OperationType getType() {
        return operationType;
    }

    public OperationTarget getTarget() {
        return operationTarget;
    }

    public void rethrowExceptionIfOccurred() throws ExecutionException {
        if (exception!=null) {
            throw new OperationExecutionException(exception);
        }
    }

    public void execute() {
        try {
            result = function.get();
        } catch(final Exception e) {
            exception = e;
        }
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    public void done() {
        done = true;
        if (countDownLatch != null) {
            countDownLatch.countDown();
        }
    }

    public boolean isAutonomous() {
        return countDownLatch == null;
    }

    public void cancel(final R result) {
        this.result = result;
        done = true;
        countDownLatch.countDown();
    }

    @Override
    public R get() throws InterruptedException, ExecutionException {
        countDownLatch.await();
        rethrowExceptionIfOccurred();
        return result;
    }

    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!countDownLatch.await(timeout, unit)) {
            throw new TimeoutException("Timeout elapsed after waiting for " + timeout + unit);
        }
        rethrowExceptionIfOccurred();
        return result;
    }
}
