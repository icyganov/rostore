package org.rostore.v2.media.block.container;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.allocator.BlockAllocator;


import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Specifies a set of blocks in the current transaction.
 * <p>All the blocks that has been made available through this object will stay
 * permanently in memory, until this block container is closed.</p>
 * <p>Commit operation will flush all the blocks from the container,
 * and mark all as disposable, and remove them from the container.</p>
 * <p>The blocks will be cached on the higher level, so if the
 * container will request them again it probably get it from this cache.</p>
 * <p>The user of this class should not preserve the instance of the blocks
 * after committing the block container.</p>
 * <p>Container contains its own internal cache of all blocks,
 * so it is smart just to use the block indices if required,
 * the block objects should rather be requested by {@link #getBlock(long, BlockType)}</p>
 * <p>See also {@link org.rostore.v2.media.block.BlockProvider}</p>
 */
public class BlockContainer implements Committable {

    private final Map<Long, Block> blocks = new HashMap<>();

    private final int containerId;

    private final Media media;

    private Status status;

    //private StackTraceElement[] elems;

    /**
     * Checks if the block with the given index is in this container
     *
     * @param blockIndex the block index
     * @return {@code true} if the block is already opened in this container
     */
    public boolean hasBlock(final long blockIndex) {
        return blocks.containsKey(blockIndex);
    }

    /**
     * A media object this container belongs to
     *
     * @return the media object
     */
    public Media getMedia() {
        return media;
    }

    /**
     * A unique container id
     * @return the id
     */
    public int getContainerId() {
        return containerId;
    }

    /**
     * This should never be used directly
     * Use {@link Media#newBlockContainer()} instead or {@link org.rostore.v2.media.block.InternalBlockProvider#create(BlockAllocator)}
     *
     * @param media the parent media object
     * @param containerId a container id
     */
    public BlockContainer(final Media media, final int containerId) {
        this.containerId = containerId;
        this.media = media;
        this.status = Status.OPENED;
        //this.elems = new Exception().getStackTrace();
    }

    /**
     * The number of blocks in the container
     * @return number of opened blocks
     */
    public int size() {
        return blocks.size();
    }

    /**
     * Provides a block to read and write
     *
     * @param index the index of the block
     * @param blockType a type of the block
     *
     * @return the block associated with the container
     */
    public Block getBlock(final long index, final BlockType blockType) {
        checkOpened();
        Block block = blocks.get(index);
        if (block == null) {
            block = media.getMappedPhysicalBlocks().get(this, index, blockType);
            blocks.put(index, block);
        }
        return block;
    }

    /**
     * Evicts a block from the current container
     *
     * <p>Never use this function directly.</p>
     *
     * @param block the block to evict
     */
    public void evict(final Block block) {
        checkOpened();
        if (!blocks.containsKey(block.getAbsoluteIndex())) {
            throw new RoStoreException("Can't evict unloaded block " + block.getAbsoluteIndex());
        }
        blocks.remove(block.getAbsoluteIndex());
        media.getMappedPhysicalBlocks().remove(this, block.getAbsoluteIndex());
    }

    /**
     * Checks if the block is loaded within this container,
     * and then evict it.
     *
     * @param blockIndex the block index to evice
     */
    public void evictIfLoaded(final long blockIndex) {
        checkOpened();
        final Block block = blocks.remove(blockIndex);
        if (block != null) {
            media.getMappedPhysicalBlocks().remove(this, block.getAbsoluteIndex());
        }
    }

    /**
     * Close all the blocks and the instance of block container.
     */
    @Override
    public void close() {
        checkOpened();
        commit();
        this.status = Status.CLOSED;
        media.freeBlockContainer(containerId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() {
        checkOpened();
        do {
            final Collection<Block> current = blocks.values();
            if (current.isEmpty()) {
                return;
            }
            current.iterator().next().close();
        } while (true);
    }

    @Override
    public Status getStatus() {
        return status;
    }

}
