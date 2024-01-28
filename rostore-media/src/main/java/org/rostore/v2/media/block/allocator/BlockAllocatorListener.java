package org.rostore.v2.media.block.allocator;

import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.block.BlockType;

/**
 * An instance of this listener can be provided to {@link BlockAllocator},
 * which will be notified if the blocks are freed or allocated
 */
public interface BlockAllocatorListener {

    /**
     * Notifies that the blocks have been freed
     *
     * @param name the name of the allocator
     * @param catalogBlockIndices the indices to be freed
     * @param rebalance if the rebalancing is requested
     */
    void blocksFreed(final String name, final CatalogBlockIndices catalogBlockIndices, final boolean rebalance);

    /**
     * Notifies that the blocks have been allocated
     *
     * @param name the name of the allocator
     * @param catalogBlockIndices the indices to be allocated
     * @param rebalance if the rebalancing is requested
     */
    void blocksAllocated(final String name, final BlockType blockType, final CatalogBlockIndices catalogBlockIndices, final boolean rebalance);

}
