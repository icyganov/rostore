package org.rostore.v2.media.block.allocator;

import org.rostore.entity.MemoryAllocation;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.MemoryConsumption;
import org.rostore.v2.media.block.BlockType;

/**
 * This interface allows to allocate and free blocks
 * What rebalance is?
 * The allocation involves usage of blocks where the catalog is stored. The process allocates some blocks in advance
 * to not have a need to allocate them during the allocation / free operation itself. So this already allocated
 * blocks are used to maintain a chain on catalog blocks and can extend or shrink it.
 * After the operation is done it may have used one or several of that block, so that the "balance" of
 * the preserved blocks is changed. The balance=true would mean that the operation must execute the
 * rebalance at the end of operation. The true is all the time provided if allocation / freeing is executed
 * by the customer...
 * But during the rebalancing itself the rebalance should be suppressed to not fall into the recursion problem.
 * That's why the rebalance=false is all the time used from the internal processes.
 * <p>To simplify: customer execute operation with rebalance = true, internal proceses requesting the blocks
 * are using rebalance=false</p>
 */
public interface BlockAllocatorInternal extends Closeable {

    MemoryAllocation getMemoryAllocation();

    long getFreeBlocks();

    /**
     * provide a list of blocks to be used
     * @param blockNumber the number of blocks
     * @param rebalance if rebalance should happen in this cycle
     * @return the block ids
     */
    CatalogBlockIndices allocate(final BlockType blockType, int blockNumber, boolean rebalance);

    /**
     * Allocate just one block
     *
     * @param rebalance if the rebalance should happen in this cycle
     * @return the block index
     */
    long allocate(final BlockType blockType, boolean rebalance);

    /**
     * Marks a provided block as free
     *
     * @param blockIndex the block index to mark
     * @param rebalance if the rebalance should happend in this cycle
     */
    void free(long blockIndex, boolean rebalance);

    /**
     * Free a set of blocks
     *
     * @param indices the set of blocks to free
     * @param rebalance a flag if rebalance should happen
     */
    void free(final CatalogBlockIndices indices, boolean rebalance);

    void dump();

    /**
     * Remove all blocks managed by this block allocator
     */
    void remove();

    /**
     * Return the first block of the allocator
     *
     * @return the block index
     */
    long getStartIndex();

    /**
     * The parent media
     *
     * @return the media object that this allocator belongs to
     */
    Media getMedia();

    /**
     * The name of the allocator
     *
     * @return the name of the allocator
     */
    String getName();
}


