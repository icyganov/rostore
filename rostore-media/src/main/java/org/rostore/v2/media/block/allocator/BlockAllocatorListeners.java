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

    public void addListener(final BlockAllocatorListener blockAllocatorListener) {
        blockAllocatorListeners.add(blockAllocatorListener);
    }

    public void removeListener(final BlockAllocatorListener blockAllocatorListener) {
        blockAllocatorListeners.remove(blockAllocatorListener);
    }

    public boolean isEnabled() {
        return !blockAllocatorListeners.isEmpty();
    }

    public void notifyAllocated(final String name, final BlockType blockType, final CatalogBlockIndices catalogBlockIndices, boolean rebalance) {
        for (BlockAllocatorListener listener : blockAllocatorListeners) {
            listener.blocksAllocated(name, blockType, catalogBlockIndices, rebalance);
        }
    }

    public void notifyFreed(final String name, final CatalogBlockIndices catalogBlockIndices, boolean rebalance) {
        for (BlockAllocatorListener listener : blockAllocatorListeners) {
            listener.blocksFreed(name, catalogBlockIndices, rebalance);
        }
    }

}
