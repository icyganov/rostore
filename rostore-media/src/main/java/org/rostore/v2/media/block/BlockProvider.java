package org.rostore.v2.media.block;

import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.block.container.BlockContainer;

public interface BlockProvider {

    BlockContainer getBlockContainer();

    BlockAllocator getBlockAllocator();

    Media getMedia();

    default Block allocateBlock(final BlockType blockType) {
        return getBlockContainer().getBlock(getBlockAllocator().allocate(blockType), blockType);
    }

    default void freeBlock(final long blockIndex) {
        getBlockContainer().evictIfLoaded(blockIndex);
        getBlockAllocator().free(blockIndex);
    }

}
