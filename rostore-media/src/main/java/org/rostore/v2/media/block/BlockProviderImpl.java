package org.rostore.v2.media.block;

import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.block.container.BlockContainer;

public class BlockProviderImpl implements BlockProvider {

    private BlockAllocator blockAllocator;
    private BlockContainer blockContainer;

    @Override
    public BlockContainer getBlockContainer() {
        return blockContainer;
    }

    @Override
    public BlockAllocator getBlockAllocator() {
        return blockAllocator;
    }

    public void exchangeBlockAllocator(final BlockAllocator blockAllocator) {
        this.blockAllocator = blockAllocator;
    }

    @Override
    public Media getMedia() {
        return blockContainer.getMedia();
    }

    public static BlockProviderImpl internal(final BlockAllocator blockAllocator) {
        return new BlockProviderImpl(blockAllocator);
    }

    public static BlockProviderImpl internal(final Media media) {
        return new BlockProviderImpl(media);
    }

    public static BlockProviderImpl external(final BlockProvider blockProvider) {
        return new BlockProviderImpl(blockProvider);
    }

    private BlockProviderImpl(final BlockAllocator blockAllocator) {
        this.blockAllocator = blockAllocator;
        this.blockContainer = blockAllocator.getMedia().newBlockContainer();
    }

    private BlockProviderImpl(final Media media) {
        this.blockAllocator = null;
        this.blockContainer = media.newBlockContainer();
    }

    private BlockProviderImpl(final BlockProvider blockProvider) {
        this.blockContainer = blockProvider.getBlockContainer();
        this.blockAllocator = blockProvider.getBlockAllocator();
    }
}
