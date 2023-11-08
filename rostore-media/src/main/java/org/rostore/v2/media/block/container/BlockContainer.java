package org.rostore.v2.media.block.container;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockType;


import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BlockContainer implements Committable {

    private Map<Long, Block> blocks = new HashMap<>();

    private final int containerId;

    private final Media media;

    private Status status;

    //private StackTraceElement[] elems;

    public boolean hasBlock(final long index) {
        return blocks.containsKey(index);
    }

    public Media getMedia() {
        return media;
    }

    public int getContainerId() {
        return containerId;
    }

    /**
     * This should never be used directly
     * Use {@link Media#newBlockContainer()} instead
     *
     * @param media
     * @param containerId
     */
    public BlockContainer(final Media media, final int containerId) {
        this.containerId = containerId;
        this.media = media;
        this.status = Status.OPENED;
        //this.elems = new Exception().getStackTrace();
    }

    public int size() {
        return blocks.size();
    }

    public Block getBlock(final long index, final BlockType blockType) {
        checkOpened();
        Block block = blocks.get(index);
        if (block == null) {
            block = media.getMappedPhysicalBlocks().get(this, index, blockType);
            blocks.put(index, block);
        }
        return block;
    }

    public void evict(final Block block) {
        checkOpened();
        if (!blocks.containsKey(block.getAbsoluteIndex())) {
            throw new RoStoreException("Can't evict unloaded block " + block.getAbsoluteIndex());
        }
        blocks.remove(block.getAbsoluteIndex());
        media.getMappedPhysicalBlocks().remove(this, block.getAbsoluteIndex());
    }

    public void evictIfLoaded(final long blockIndex) {
        checkOpened();
        final Block block = blocks.remove(blockIndex);
        if (block != null) {
            media.getMappedPhysicalBlocks().remove(this, block.getAbsoluteIndex());
        }
    }

    @Override
    public void close() {
        checkOpened();
        commit();
        this.status = Status.CLOSED;
        media.freeBlockContainer(containerId);
    }

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
