package org.rostore.entity;

/**
 * Information regarding memory usage
 */
public interface MemoryAllocation {

    /**
     * Get number of bytes used by the data.
     * This data is actively used by some payload.
     *
     * @return the size in bytes
     */
    long getPayloadSize();

    /**
     * Get number of bytes allocated for the data.
     * This data is locked and can't be transferred somewhere.
     *
     * Allocated size = Free size + used size or 0.
     *
     * @return the size in bytes
     */
    long getTotalLockedSize();

    /**
     * Get number of bytes that have been free from the allocated data.
     * This data can be used by the payload, but stays effectively locked unused.
     *
     * @return the size in bytes
     */
    long getLockedFreeSize();

}
