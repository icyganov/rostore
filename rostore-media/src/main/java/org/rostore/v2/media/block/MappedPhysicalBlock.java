package org.rostore.v2.media.block;

import org.rostore.v2.media.block.container.BlockContainer;

import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MappedPhysicalBlock {

    private MappedByteBuffer mappedByteBuffer;

    private final long index;

    private boolean dirty;

    // containerId => Block
    private Map<Integer, Block> blocks;

    private long unusedSince;

    private BlockType blockType;

    public Set<Integer> getAllContainerIds() {
        return blocks.keySet();
    }

    public long getUnusedSince() {
        return unusedSince;
    }

    public synchronized boolean inUse() {
        return !blocks.isEmpty();
    }

    public boolean isDirty() {
        return dirty;
    }

    public long getIndex() {
        return index;
    }

    public void flush() {
        if (dirty) {
            this.mappedByteBuffer.force();
            this.dirty = false;
        }
    }

    public MappedPhysicalBlock(final long index, final BlockType blockType) {
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
