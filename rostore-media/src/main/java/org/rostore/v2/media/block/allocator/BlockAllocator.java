package org.rostore.v2.media.block.allocator;

import org.rostore.entity.MemoryAllocation;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.container.Status;

/**
 * This interface allows to allocate and free blocks.
 *
 * <p>There are two types of the allocators: root and secondary.</p>
 * <p>Root one allocates and frees on the storage level and used to manage all the blocks in the storage,
 * where as the secondary can be created to manage some groups of blocks. All the blocks allocated
 * over the secondary one can be freed easily.</p>
 */
public class BlockAllocator implements Closeable {

    private final BlockAllocatorInternal blockAllocatorInternal;

    /**
     * Used to identify the allocator, e.g. in logs. It has no any further meaning.
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



