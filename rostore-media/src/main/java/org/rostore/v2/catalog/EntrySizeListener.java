package org.rostore.v2.catalog;

/**
 * A listener get called when number of entries in the block changes.
 */
public interface EntrySizeListener {

    /**
     * Called when the number of entries changes
     *
     * @param newSize the new size
     * @param delta how many entries has been added (positive) / removed (negative)
     */
    void apply(final long newSize, final long delta);
}
