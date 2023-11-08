package org.rostore.v2.seq;

import org.rostore.Utils;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.container.Status;

import java.util.function.Function;

/**
 *
 */
public abstract class SequenceBlock implements Closeable {

    private int index;

    private final BlockSequence blockSequence;

    public BlockSequence getBlockSequence() {
        return blockSequence;
    }

    private void throwExceptionIfInvalid(final String message) {
        if (invalid()) {
            throw new RoStoreException("The <" + this + "> is invalid: " + message);
        }
    }

    public static <T extends SequenceBlock> T load(final BlockProvider blockProvider,
                                                   final long startIndex,
                                                   final Function<BlockSequence<T>, T> factory,
                                                   final BlockType blockType) {
        return blockProvider.getMedia().getBlockIndexSequences().get(startIndex, (blockIndexSequence) -> {
            if (blockIndexSequence == null) {
                return new BlockSequence<>(blockProvider, startIndex, factory, blockType);
            } else {
                return new BlockSequence<>(blockProvider, blockIndexSequence, factory, blockType);
            }
        }).getSequenceBlock();
    }

    public static <T extends SequenceBlock> T create(final BlockProvider blockProvider,
                                                     final Function<BlockSequence<T>, T> factory,
                                                     final BlockType blockType) {
        return blockProvider.getMedia().getBlockIndexSequences().get(Utils.ID_UNDEFINED,
                (blockIndexSequence) -> {
                    CatalogBlockIndices catalogBlockIndices = blockProvider.getBlockAllocator().allocate(blockType, Properties.AVG_FREE_BLOCK_NUMBER);
                    return new BlockSequence<>(blockProvider, catalogBlockIndices, factory, blockType);
                }).getSequenceBlock();
    }

    public static <T extends SequenceBlock> T create(final BlockProvider blockProvider,
                                                     final CatalogBlockIndices catalogBlockIndices,
                                                     final Function<BlockSequence<T>, T> factory,
                                                     final BlockType blockType) {
        return blockProvider.getMedia().getBlockIndexSequences().get(Utils.ID_UNDEFINED,
                (blockIndexSequence) -> new BlockSequence<>(blockProvider, catalogBlockIndices, factory, blockType))
                .getSequenceBlock();
    }

    public int getHeaderSize() {
        return blockSequence.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
    }

    public void previous() {
        throwExceptionIfInvalid("previous");
        if (index > 0) {
            moveTo(index-1);
            return ;
        }
        invalidate();
    }

    public void next() {
        throwExceptionIfInvalid("next");
        if (index < blockSequence.length()-1) {
            moveTo(index+1);
            return ;
        }
        invalidate();
    }

    public void last() {
        if (blockSequence.length() != 0) {
            moveTo(blockSequence.length()-1);
        } else {
            invalidate();
        }
    }

    public void root() {
        if (blockSequence.length() != 0) {
            moveTo(0);
        } else {
            invalidate();
        }
    }

    public void invalidate() {
        index = -1;
    }

    public void moveTo(final int index) {
        if (index >= 0 && index < blockSequence.getBlockIndexSequence().length()) {
            checkOpened();
            this.index = index;
        } else {
            invalidate();
        }
    }

    public boolean isRoot() {
        throwExceptionIfInvalid("isRoot");
        return index == 0;
    }

    public int getIndex() {
        throwExceptionIfInvalid("getIndex");
        return index;
    }

    protected SequenceBlock(final BlockSequence blockSequence) {
        this.blockSequence = blockSequence;
        if (blockSequence.length() > 0) {
            moveTo(0);
        } else {
            invalidate();
        }
    }

    public boolean invalid() {
        return index == -1;
    }

    public boolean valid() {
        return index != -1;
    }

    public Block getBlock() {
        throwExceptionIfInvalid("getBlock");
        return blockSequence.getBlockByIndex(index);
    }

    /**
     * Removes the current catalog block
     * @return the block
     */
    public void delete() {
        throwExceptionIfInvalid("delete");
        blockSequence.removeFreeBlock(getIndex());
    }

    public void createNewAfter() {
        throwExceptionIfInvalid("createNewAfter");
        blockSequence.addFreeBlock(getIndex());
        next();
    }

    protected abstract boolean isUnused();

    protected void clean() {
        getBlock().clean();
    }

    /**
     * Closes an underlying sequence
     */
    public void close() {
        blockSequence.close();
    }

    public Status getStatus() {
        return blockSequence.getStatus();
    }

}
