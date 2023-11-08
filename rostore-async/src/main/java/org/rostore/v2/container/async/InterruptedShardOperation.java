package org.rostore.v2.container.async;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class InterruptedShardOperation<R> implements Future<R> {

    public static final InterruptedShardOperation INTERRUPTED_SHARD_OPERATION = new InterruptedShardOperation();

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return true;
    }

    @Override
    public boolean isCancelled() {
        return true;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public R get() throws InterruptedException {
        throw new AsyncContainerAccessException("Shard is shutdown, operation has been interrupted.");
    }

    @Override
    public R get(long timeout, TimeUnit unit) throws InterruptedException {
        throw new AsyncContainerAccessException("Shard is shutdown, operation has been interrupted.");
    }
}
