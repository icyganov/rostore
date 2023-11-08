package org.rostore.v2.container.async;

import org.rostore.entity.Record;
import org.rostore.entity.RoStoreException;
import org.rostore.entity.StreamProcessingException;

import java.util.concurrent.*;
import java.util.function.Consumer;

public class AsyncStream<S extends AutoCloseable> implements AutoCloseable, Future<S> {

    private final S stream;
    private Exception exception;

    private AsyncStatus status;
    private CountDownLatch countDownLatch;
    private AsyncListener asyncListener;

    public static <S extends AutoCloseable> AsyncStream<S> wrap(final S s) {
        return wrap(s, null);
    }

    public static <S extends AutoCloseable> AsyncStream<S> wrap(final S s, final AsyncListener asyncListener) {
        return new AsyncStream<>(s, false, asyncListener);
    }

    public static <S extends AutoCloseable> AsyncStream<S> wrapBlocking(final S s) {
        return wrapBlocking(s, null);
    }

    public static <S extends AutoCloseable> AsyncStream<S> wrapBlocking(final S s, final AsyncListener asyncListener) {
        return new AsyncStream<>(s, true, asyncListener);
    }

    public Exception getException() {
        return exception;
    }

    public void notifyRecord(final Record record) {
        if (asyncListener != null) {
            asyncListener.record(record);
        }
    }

    /**
     * This function is called from the async process to process the entity.
     * This function will safely mark the async process as done and
     * can only be executed once.
     *
     * @param runnable the callback the async process implements
     */
    public final void processFunction(final AsyncFunction<S> runnable) {
        start();
        try {
            runnable.process(stream);
            status = AsyncStatus.SUCCESS;
            if (asyncListener != null) {
                asyncListener.status(status);
            }
        } catch (final Exception e) {
            status = AsyncStatus.ERROR;
            this.exception = e;
            StreamProcessingException streamProcessingException = new StreamProcessingException(e);
            if (asyncListener != null) {
                asyncListener.error(e);
                asyncListener.status(status);
            }
            throw streamProcessingException;
        } finally {
            if (countDownLatch != null) {
                countDownLatch.countDown();
            }
        }
    }

    public void fail(final Exception e) {
        status = AsyncStatus.ERROR;
        if (asyncListener != null) {
            asyncListener.error(e);
            asyncListener.status(status);
        }
    }

    private AsyncStream(final S s, final boolean blocking, final AsyncListener asyncListener) {
        this.stream = s;
        this.status = AsyncStatus.OPENED;
        this.asyncListener = asyncListener;
        if (blocking) {
            this.countDownLatch = new CountDownLatch(1);
        }
        if (asyncListener != null) {
            asyncListener.status(status);
        }
    }

    @Override
    public boolean cancel(boolean b) {
        this.status = AsyncStatus.CANCELED;
        if (asyncListener != null) {
            asyncListener.status(status);
        }
        if (countDownLatch != null) {
            countDownLatch.countDown();
        }
        return true;
    }

    @Override
    public boolean isCancelled() {
        return status == AsyncStatus.CANCELED;
    }

    public boolean isDone() {
        return status.isFinished();
    }

    private void start() {
        if (status == AsyncStatus.OPENED) {
            status = AsyncStatus.STARTED;
            if (asyncListener != null) {
                asyncListener.status(status);
            }
            return;
        }
        throw new RoStoreException("The stream is not in opened state.");
    }

    @Override
    public S get(long l, TimeUnit timeUnit) {
        if (countDownLatch != null) {
            try {
                countDownLatch.await(l, timeUnit);
                if (exception != null) {
                    throw new AsyncException(exception);
                }
                return stream;
            } catch (InterruptedException e) {
                throw new RoStoreException("Interrupted while waiting for stream", e);
            }
        } else {
            throw new RoStoreException("Can't wait for a non-blocking stream");
        }
    }

    /**
     * This one will only work if the object is created as blocking
     * @return
     */
    public S get() {
        if (countDownLatch != null) {
            try {
                countDownLatch.await();
                if (exception != null) {
                    throw new AsyncException(exception);
                }
                return stream;
            } catch (InterruptedException e) {
                throw new RoStoreException("Interrupted while waiting for stream", e);
            }
        } else {
            throw new RoStoreException("Can't wait for a non-blocking stream");
        }
    }

    @Override
    public void close() throws Exception {
        stream.close();
    }

    public void empty() {
        if (status != AsyncStatus.OPENED) {
            throw new RoStoreException("Try to mark the stream as empty after it has been started.");
        }
        status = AsyncStatus.SUCCESS;
        if (countDownLatch != null) {
            countDownLatch.countDown();
        }
        if (asyncListener != null) {
            asyncListener.status(status);
        }
    }
}
