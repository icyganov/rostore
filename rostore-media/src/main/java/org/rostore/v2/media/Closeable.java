package org.rostore.v2.media;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.media.block.container.Status;

/**
 * An interface that marks an entity that holds opened blocks,
 * which should be closed after its usage.
 */
public interface Closeable extends AutoCloseable {
    /**
     * Closes this entity and all related blocks
     */
    void close();

    /**
     * Provides a status of this entity
     *
     * @return the status
     */
    Status getStatus();

    default void checkOpened() {
        if (getStatus() == Status.OPENED) {
            return;
        }
        throw new RoStoreException("This object has already been closed");
    }

}
