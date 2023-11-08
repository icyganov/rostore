package org.rostore.v2.media;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.media.block.container.Status;

public interface Closeable extends AutoCloseable {
    void close();

    Status getStatus();

    default void checkOpened() {
        if (getStatus() == Status.OPENED) {
            return;
        }
        throw new RoStoreException("This object has already been closed");
    }

}
