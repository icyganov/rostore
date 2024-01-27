package org.rostore.v2.media.block;

import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.block.container.BlockContainer;

/**
 * A structure that combines both {@link BlockAllocator} and {@link BlockContainer}.
 * <p>In many classes it allows to split the low-level transaction blocks collection ({@link BlockContainer})
 * and their intended allocation approach ({@link BlockAllocator} from the high-level
 * operation.</p>
 */
public interface BlockProvider {

    /**
     * Provides a block container.
     * <p>Used to account the set of blocks that participate in one specific transaction.</p>
     *
     * @return the block container
     */
    BlockContainer getBlockContainer();

    /**
     * Provides a block allocator
     * <p>Used to allocate new blocks from the set of free blocks nad mark them as used.</p>
     *
     * @return the block allocator
     */
    BlockAllocator getBlockAllocator();

    /**
     * Provides an associated {@link Media} object
     * @return the media object
     */
    Media getMedia();

    /**
     * Allocates a block with the help of {@link BlockAllocator}
     * and adds it to the associated {@link BlockContainer}.
     * @param blockType a block type
     * @return the block that has been allocated and added to the block container
     */
    default Block allocateBlock(final BlockType blockType) {
        return getBlockContainer().getBlock(getBlockAllocator().allocate(blockType), blockType);
    }

    /**
     * Frees a block from the allocator and removes it from the container.
     *
     * @param blockIndex the index of the block
     */
    default void freeBlock(final long blockIndex) {
        getBlockContainer().evictIfLoaded(blockIndex);
        getBlockAllocator().free(blockIndex);
    }

}
