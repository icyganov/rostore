package org.rostore.v2.media;

import org.rostore.v2.media.block.container.Status;

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
