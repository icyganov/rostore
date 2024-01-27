package org.rostore.v2.media.block;

import org.rostore.v2.media.block.container.BlockContainer;

import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents the mapped to the memory the physical block of the storage.
 *
 * <p>It should not be modified explicitly, instead the user processes
 * should retrieve the {@link Block}, which will contain the data of this
 * block, but provide an independent pointer to the block, so it can
 * be modified independently.</p>
 *
 * <p>This object holds all the references to the {@link Block} it was duplicated to
 * as well as the information regaring the {@link BlockContainer} these duplicates are opened with.</p>
 */
public class MappedPhysicalBlock {

    private MappedByteBuffer mappedByteBuffer;

    private final long index;

    private boolean dirty;

    // containerId => Block
    private Map<Integer, Block> blocks;

    private long unusedSince;

    private BlockType blockType;

    /**
     * Provides all container ids where this physical block is used.
     *
     * @return a set of container ids
     */
    public Set<Integer> getAllContainerIds() {
        return blocks.keySet();
    }

    /**
     * Provides a timestamp when this block become unused.
     *
     * @return a unix epoch timestamp in milliseconds
     */
    public long getUnusedSince() {
        return unusedSince;
    }

    /**
     * Provides an indication if this block is used in at least one active process
     *
     * @return {@code true} if the block is still in use
     */
    public synchronized boolean inUse() {
        return !blocks.isEmpty();
    }

    /**
     * Provides an indication if this block has been modified in memory,
     * but has not been explicitly flushed to the persistence layer.
     *
     * <p>This is a weak indicator, the operation of flushing might be executed by
     * the underlying processes in the operating system, which do not have a feedback loop to the Ro-Store.</p>
     *
     * @return {@code true} if the block is marked by the ro-store as dirty
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * The index of the block in the ro-store.
     *
     * @return the index of the block
     */
    public long getIndex() {
        return index;
    }

    /**
     * Execute a flushing of the current state of the block
     * back to the storage.
     * <p>The dirty flag will be reset.</p>
     */
    public void flush() {
        if (dirty) {
            this.mappedByteBuffer.force();
            this.dirty = false;
        }
    }

    protected MappedPhysicalBlock(final long index, final BlockType blockType) {
        this.index = index;
        this.blockType = blockType;
        dirty = false;
        blocks = new HashMap<>();
    }

    /**
     * This marks the block as used (inUse = true)
     * @param blockContainer
     */
    protected synchronized void markAsUsed(final BlockContainer blockContainer) {
        blocks.put(blockContainer.getContainerId(), null);
    }

    protected void setBlockType(final BlockType blockType) {
        this.blockType = blockType;
    }

    public BlockType getBlockType() {
        return blockType;
    }

    protected synchronized Block get(final BlockContainer blockContainer) {
        if (mappedByteBuffer == null) {
            mappedByteBuffer = blockContainer.getMedia().map(index);
        }
        Block block = blocks.get(blockContainer.getContainerId());
        if (block == null) {
            block = new Block(mappedByteBuffer.duplicate(),
                    this,
                    blockContainer);
            blocks.put(blockContainer.getContainerId(), block);
        }
        return block;
    }

    protected void setDirty() {
        dirty = true;
    }

    protected synchronized void remove(final BlockContainer blockContainer) {
        blocks.remove(blockContainer.getContainerId());
        if (!inUse()) {
            unusedSince = System.currentTimeMillis();
        }
    }

    protected boolean isEmpty() {
        return blocks.isEmpty();
    }
}
