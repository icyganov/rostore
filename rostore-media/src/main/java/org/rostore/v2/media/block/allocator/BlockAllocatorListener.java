package org.rostore.v2.media.block.allocator;

import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.block.BlockType;

public interface BlockAllocatorListener {

    void blocksFreed(final String name, final CatalogBlockIndices catalogBlockIndices, final boolean rebalance);

    void blocksAllocated(final String name, final BlockType blockType, final CatalogBlockIndices catalogBlockIndices, final boolean rebalance);

}
