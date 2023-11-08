package org.rostore.v2.media.block.allocator;

import org.rostore.entity.MemoryAllocation;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.container.Status;

/**
 * This interface allows to allocate and free blocks
 */
public class BlockAllocator implements Closeable {

    private final BlockAllocatorInternal blockAllocatorInternal;

    /**
     * Used to identify the allocator
     */

    public String getName() {
        return blockAllocatorInternal.getName();
    }

    public static BlockAllocator wrap(final BlockAllocatorInternal blockAllocatorInternal) {
        return new BlockAllocator(blockAllocatorInternal);
    }

    public MemoryAllocation getMemoryAllocation() {
        return blockAllocatorInternal.getMemoryAllocation();
    }

    public BlockAllocatorInternal getBlockAllocatorInternal() {
        return blockAllocatorInternal;
    }

    private BlockAllocator(BlockAllocatorInternal blockAllocatorInternal) {
        this.blockAllocatorInternal = blockAllocatorInternal;
    }

    public long getFreeBlocks() {
        return blockAllocatorInternal.getFreeBlocks();
    }

    public CatalogBlockIndices allocate(final BlockType blockType, int blockNumber) {
        CatalogBlockIndices ret = blockAllocatorInternal.allocate(blockType, blockNumber, true);
        return ret;
    }

    public long allocate(final BlockType blockType) {
        return blockAllocatorInternal.allocate(blockType, true);
    }

    public void free(long blockIndex) {
        blockAllocatorInternal.free(blockIndex, true);
    }

    public void free(CatalogBlockIndices indices) {
        blockAllocatorInternal.free(indices, true);
    }

    @Override
    public void close() {
        blockAllocatorInternal.close();
    }

    @Override
    public Status getStatus() {
        return null;
    }

    public long getStartIndex() {
        return blockAllocatorInternal.getStartIndex();
    }

    public void remove() {
        blockAllocatorInternal.remove();
    }

    public Media getMedia() {
        return blockAllocatorInternal.getMedia();
    }
}



