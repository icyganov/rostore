package org.rostore.v2.media.block.allocator;

import org.rostore.entity.BlockAllocation;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.container.Status;
import org.rostore.v2.seq.BlockSequence;

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
     * Used to identify the allocator, e.g. in logs. It has no any further meaning as naming this entity.
     */
    public String getName() {
        return blockAllocatorInternal.getName();
    }

    /**
     * Wraps {@link BlockAllocatorInternal} to create an allocator.
     * <p>The internal version is the one that exposes for each function a rebalance flag,
     * which is needed to cover the allocation peculiarities, described in {@link BlockSequence#rebalance()}</p>
     * <p>The internal version implements the heavy-lifting operation, and this class is
     * only hides the rebablancing flag, as all external calls should be executed with rebalance=true flag.</p>
     *
     * @param blockAllocatorInternal the internal entity that implements allocator's logic
     *
     * @return the newly created block allocator
     */
    public static BlockAllocator wrap(final BlockAllocatorInternal blockAllocatorInternal) {
        return new BlockAllocator(blockAllocatorInternal);
    }

    /**
     * Provides information about block allocation within this block allocator
     *
     * @return the block allocation
     */
    public BlockAllocation getBlockAllocation() {
        return blockAllocatorInternal.getBlockAllocation();
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

    /**
     * Allocates a given number of blocks.
     *
     * <p>The call is executed on the best effort basis. If quotas are reached an exception can be thrown.</p>
     *
     * @param blockType the block type
     * @param blockNumber the number of blocks to allocate
     * @return a set of blocks that has been allocated
     */
    public CatalogBlockIndices allocate(final BlockType blockType, int blockNumber) {
        CatalogBlockIndices ret = blockAllocatorInternal.allocate(blockType, blockNumber, true);
        return ret;
    }

    /**
     * Allocate one block
     *
     * @param blockType the type of the block
     * @return the block index of the block
     */
    public long allocate(final BlockType blockType) {
        return blockAllocatorInternal.allocate(blockType, true);
    }

    /**
     * Marks the provided block as free
     *
     * @param blockIndex the block index
     */
    public void free(long blockIndex) {
        blockAllocatorInternal.free(blockIndex, true);
    }

    /**
     * Marks all the blocks in the provided index as free
     *
     * @param indices a set of block indices to free
     */
    public void free(final CatalogBlockIndices indices) {
        blockAllocatorInternal.free(indices, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        blockAllocatorInternal.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Status getStatus() {
        return null;
    }

    /**
     * Provides the first block index of the allocator itself
     *
     * <p>Can be used to persist and restore the block allocator.</p>
     *
     * @return the block index
     */
    public long getStartIndex() {
        return blockAllocatorInternal.getStartIndex();
    }

    /**
     * Frees all the blocks managed by the allocator.
     *
     * <p>Only useful for secondary allocator. Root would not do anything as a reaction to this call.</p>
     */
    public void remove() {
        blockAllocatorInternal.remove();
    }

    /**
     * Provides parent media
     *
     * @return the hosting object
     */
    public Media getMedia() {
        return blockAllocatorInternal.getMedia();
    }
}



