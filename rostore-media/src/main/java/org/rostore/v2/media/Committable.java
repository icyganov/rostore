package org.rostore.v2.media;

/**
 * Marks an entity that holds active blocks, which needs to be committed.
 * When committed all the blocks are effectively made garbage-collectable
 * and the blocks are flushed to the underlying physical storage.
 * <p>Ro-Store caches the recent blocks, so if they get accessed by some other process
 * or be requested again by the same process they might come again from the cache.</p>
 * <p>So, this operation enforces a flush of data, and make all active blocks disappear
 * from the current context.</p>
 */
public interface Committable extends Closeable {
    /**
     * Operation commits all the active blocks.
     * It will mark the block to be garbage-collected,
     * the data from the associated physical block will be flushed to the persistent storage.
     */
    void commit();
}
