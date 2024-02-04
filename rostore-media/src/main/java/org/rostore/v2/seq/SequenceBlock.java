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
 * Represents a block that is embedded in {@link BlockSequence}.
 *
 * <p>This is wrapper object of the block. To move from one block
 * to another the {@link #moveTo(int)} is used. The instance of the
 * block is reused, but it points to another block.</p>
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

    /**
     * Loads block sequence and initialize a specific kind of sequence block on its basis.
     *
     * @param blockProvider the block provider be used in the sequence operations
     * @param startIndex the index of the first block in the sequence
     * @param factory the factory that creates a specific variant of sequence block based on the sequence
     * @param blockType the block type of the blocks belonging to the sequence
     * @return a sequence block as created by the factory
     * @param <T> the type of the sequence's block inherited object
     */
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

    /**
     * Creates a new block sequence and initialize a sequence block on its basis
     *
     * @param blockProvider the block provider be used in the sequence operations, allocation of the blocks for the sequence will also
     *                      be requested over the associated allocator {@link BlockProvider#getBlockAllocator()}
     * @param factory the factory that creates a specific variant of sequence block based on the sequence
     * @param blockType the block type of the blocks belonging to the sequence
     * @return a sequence block as created by the factory
     * @param <T> the type of the sequence's block inherited object
     */
    public static <T extends SequenceBlock> T create(final BlockProvider blockProvider,
                                                     final Function<BlockSequence<T>, T> factory,
                                                     final BlockType blockType) {
        return blockProvider.getMedia().getBlockIndexSequences().get(Utils.ID_UNDEFINED,
                (blockIndexSequence) -> {
                    CatalogBlockIndices catalogBlockIndices = blockProvider.getBlockAllocator().allocate(blockType, Properties.AVG_FREE_BLOCK_NUMBER);
                    return new BlockSequence<>(blockProvider, catalogBlockIndices, factory, blockType);
                }).getSequenceBlock();
    }

    /**
     * Creates a new block sequence and initialize a sequence block on its basis
     *
     * @param blockProvider the block provider be used in the sequence operations, allocation of the blocks for the sequence will also
     *                      be requested over the associated allocator {@link BlockProvider#getBlockAllocator()}
     * @param catalogBlockIndices the initial set of blocks that will be used by the sequence
     * @param factory the factory that creates a specific variant of sequence block based on the sequence
     * @param blockType the block type of the blocks belonging to the sequence
     * @return a sequence block as created by the factory
     * @param <T> the type of the sequence's block inherited object
     */
    public static <T extends SequenceBlock> T create(final BlockProvider blockProvider,
                                                     final CatalogBlockIndices catalogBlockIndices,
                                                     final Function<BlockSequence<T>, T> factory,
                                                     final BlockType blockType) {
        return blockProvider.getMedia().getBlockIndexSequences().get(Utils.ID_UNDEFINED,
                (blockIndexSequence) -> new BlockSequence<>(blockProvider, catalogBlockIndices, factory, blockType))
                .getSequenceBlock();
    }

    /**
     * The size of the header of the current block
     * @return the size of header in bytes
     */
    public int getHeaderSize() {
        return blockSequence.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
    }

    /**
     * Moves the pointer to the previous block in the sequence.
     */
    public void previous() {
        throwExceptionIfInvalid("previous");
        if (index > 0) {
            moveTo(index-1);
            return ;
        }
        invalidate();
    }

    /**
     * Moves the pointer to the next block in the sequence.
     */
    public void next() {
        throwExceptionIfInvalid("next");
        if (index < blockSequence.length()-1) {
            moveTo(index+1);
            return ;
        }
        invalidate();
    }

    /**
     * Moves the pointer to the last used block in the sequence.
     */
    public void last() {
        if (blockSequence.length() != 0) {
            moveTo(blockSequence.length()-1);
        } else {
            invalidate();
        }
    }

    /**
     * Moves the pointer to the root block in the sequence.
     */
    public void root() {
        if (blockSequence.length() != 0) {
            moveTo(0);
        } else {
            invalidate();
        }
    }

    /**
     * Moves the pointer to invalid index.
     */
    public void invalidate() {
        index = -1;
    }

    /**
     * Moves the pointer to the specific block in the sequence
     * @param seqIndex the relative index in the sequence of the block
     */
    public void moveTo(final int seqIndex) {
        if (seqIndex >= 0 && seqIndex < blockSequence.getBlockIndexSequence().length()) {
            checkOpened();
            this.index = seqIndex;
        } else {
            invalidate();
        }
    }

    /**
     * Checks if the current block is the first block of the sequence
     * @return {@code true} if the pointer points to the first block
     */
    public boolean isRoot() {
        throwExceptionIfInvalid("isRoot");
        return index == 0;
    }

    /**
     * Provides the current pointer where it points
     *
     * @return the index in the sequence this sequence block is currently pointing to
     */
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

    /**
     * Checks if the pointer points to invalid block
     *
     * @return {@code true} if block is invalid
     */
    public boolean invalid() {
        return index == -1;
    }

    /**
     * Checks if the pointer points to valid block
     *
     * @return {@code true} if block is valid
     */
    public boolean valid() {
        return index != -1;
    }

    /**
     * Get the block this sequence's block internal pointer points to
     * @return block
     */
    public Block getBlock() {
        throwExceptionIfInvalid("getBlock");
        return blockSequence.getBlockByIndex(index);
    }

    /**
     * Removes the current block and mark it as free.
     * <p>Function deallocates to the sequence's space.</p>
     */
    public void delete() {
        throwExceptionIfInvalid("delete");
        blockSequence.removeFreeBlock(getIndex());
    }

    /**
     * Creates a new block after the current one
     * and move to it.
     * <p>Allocation of the free block happens solely from the free blocks
     * already reserved in the sequence.</p>
     */
    public void createNewAfter() {
        throwExceptionIfInvalid("createNewAfter");
        blockSequence.addFreeBlock(getIndex());
        next();
    }

    /**
     * Provides an indication if current block does not contain any data
     *
     * @return {@code true} if no entries are currently added to block
     */
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

    /**
     * Provides a status of the block sequence
     *
     * @return the status of the block sequence
     */
    public Status getStatus() {
        return blockSequence.getStatus();
    }

}
