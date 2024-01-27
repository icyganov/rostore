package org.rostore.v2.media.block;

import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.block.container.BlockContainer;

/**
 * This instance manages a low-level block operation and creates a new {@link BlockContainer}.
 * <p>This effectively creates a transactional boundary, that needs to be managed.</p>
 * Code that creates this {@link BlockProvider} is responsible to clean it up by calling {@link BlockContainer#close()}.
 *
 * <p>It is meant to be only used in the entities that need to manage its own transactional boundary,
 * and do not expect that the {@link BlockContainer} will be provided from the outside.</p>
 *
 * <p>It is usually a major hull-like entity, that manages several other slave-entities
 * that would need a transactional boundaries provided from this overarching one.</p>
 *
 */
public class InternalBlockProvider implements BlockProvider {

    private BlockAllocator blockAllocator;
    private BlockContainer blockContainer;

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockContainer getBlockContainer() {
        return blockContainer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockAllocator getBlockAllocator() {
        return blockAllocator;
    }

    /**
     * Exchange a block allocator.
     * Usually should not be excessively used, only in places when the allocator can't be known
     * at the moment of the object creation.
     *
     * @param blockAllocator
     */
    public void exchangeBlockAllocator(final BlockAllocator blockAllocator) {
        this.blockAllocator = blockAllocator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Media getMedia() {
        return blockContainer.getMedia();
    }

    /**
     * Creates a block provider that contains a new {@link BlockContainer}.
     *
     * @param blockAllocator the allocator to be used
     * @return an instance of the {@link BlockProvider}
     */
    public static InternalBlockProvider create(final BlockAllocator blockAllocator) {
        return new InternalBlockProvider(blockAllocator);
    }

    /**
     * Creates a block provider that contains a new {@link BlockContainer}.
     *
     * <p>Allocator is not set in this one and needs to be associated with {@link #exchangeBlockAllocator(BlockAllocator)}</p>
     *
     * @param media the media object
     * @return an instance of the {@link BlockProvider}
     */
    public static InternalBlockProvider create(final Media media) {
        return new InternalBlockProvider(media);
    }

    private InternalBlockProvider(final BlockAllocator blockAllocator) {
        this.blockAllocator = blockAllocator;
        this.blockContainer = blockAllocator.getMedia().newBlockContainer();
    }

    private InternalBlockProvider(final Media media) {
        this.blockAllocator = null;
        this.blockContainer = media.newBlockContainer();
    }

}
