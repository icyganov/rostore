package org.rostore.v2.media;

import org.rostore.v2.media.block.container.Status;

/**
 * A basic class that has the status and can be closed.
 *
 * <p>This is the most simple implementation of {@link Closeable}</p>
 */
public class RootClosableImpl implements Closeable {

    private Status status = Status.OPENED;

    @Override
    public void close() {
        checkOpened();
        status = Status.CLOSED;
    }

    @Override
    public Status getStatus() {
        return status;
    }
}
