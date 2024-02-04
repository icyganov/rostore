package org.rostore.v2.media.block.allocator;

import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.block.BlockType;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the holder of all block allocator's listeners.
 */
public class BlockAllocatorListeners {

    private final List<BlockAllocatorListener> blockAllocatorListeners;

    public BlockAllocatorListeners() {
        this.blockAllocatorListeners = new ArrayList<>();
    }

    /**
     * Add listener to be notified
     *
     * @param blockAllocatorListener the listener to add
     */
    public void addListener(final BlockAllocatorListener blockAllocatorListener) {
        blockAllocatorListeners.add(blockAllocatorListener);
    }

    /**
     * Remove the listener, and stop receiving notifications
     *
     * @param blockAllocatorListener the listener to disable
     */
    public void removeListener(final BlockAllocatorListener blockAllocatorListener) {
        blockAllocatorListeners.remove(blockAllocatorListener);
    }

    /**
     * Enabled if at least one listener is added
     *
     * @return {@code true} if at least one listener is added
     */
    public boolean isEnabled() {
        return !blockAllocatorListeners.isEmpty();
    }

    /**
     * Notify all listeners in the collection that a set of new blocks has just been allocated
     * @param name the name of the allocator
     * @param blockType the type of the allocated block
     * @param catalogBlockIndices a set of allocated blocks
     * @param rebalance {@code true} if allocation has happened in the rebarance=true cycle
     */
    public void notifyAllocated(final String name, final BlockType blockType, final CatalogBlockIndices catalogBlockIndices, final boolean rebalance) {
        for (BlockAllocatorListener listener : blockAllocatorListeners) {
            listener.blocksAllocated(name, blockType, catalogBlockIndices, rebalance);
        }
    }

    /**
     * Notify all listeners in the collection that a set of blocks has just been freed
     * @param name the name of the allocator
     * @param catalogBlockIndices a set of allocated blocks
     * @param rebalance {@code true} if blocks have been freed in the rebarance=true cycle
     */
    public void notifyFreed(final String name, final CatalogBlockIndices catalogBlockIndices, final boolean rebalance) {
        for (BlockAllocatorListener listener : blockAllocatorListeners) {
            listener.blocksFreed(name, catalogBlockIndices, rebalance);
        }
    }

}
