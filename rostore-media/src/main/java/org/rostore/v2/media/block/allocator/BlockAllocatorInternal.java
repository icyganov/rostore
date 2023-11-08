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
     * @return
     */
    CatalogBlockIndices allocate(final BlockType blockType, int blockNumber, boolean rebalance);

    /**
     * allocate just one block
     * @param rebalance if the rebalance should happen in this cycle
     * @return
     */
    long allocate(final BlockType blockType, boolean rebalance);

    /**
     * Marks a provided block as free
     * @param blockIndex the block index to mark
     * @param rebalance if the rebalance should happend in this cycle
     */
    void free(long blockIndex, boolean rebalance);

    void free(final CatalogBlockIndices indices, boolean rebalance);

    void dump();

    void remove();

    long getStartIndex();

    Media getMedia();

    String getName();
}


