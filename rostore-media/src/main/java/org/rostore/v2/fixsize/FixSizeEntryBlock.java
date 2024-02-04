package org.rostore.v2.fixsize;

import org.rostore.Utils;
import org.rostore.v2.catalog.EntrySizeListener;
import org.rostore.v2.media.block.Block;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.seq.BlockSequence;
import org.rostore.v2.seq.SequenceBlock;

import java.util.function.Function;

/**
 * This is an extension of sequence block that maps the block area as
 * a set of the fixed-sized entries.
 * <p>All the entries are stored at the beginning of the block, so the used area of block differs from block to block.</p>
 * <p>Block stores the number of entries that has been added to the block.</p>
 * <p>The nature of the entries and their size can be different, but once set stays the same in all blocks of the sequence.</p>
 * <p>The block allows to store an extra header. The header size differentiates between the
 * first block (the header of the first block in the sequence) and
 * the regular block's header. This allow to store additional data before all the entries in the block.</p>
 *
 * @param <T> a class representing the entries
 */
public class FixSizeEntryBlock<T extends FixSizeEntry> extends SequenceBlock {

    private final T fixSizeEntry;
    private int firstHeaderSize;
    private int bytesPerEntryNumber;

    private final EntrySizeListener newEntryNumberListener;

    /**
     * A block provider based on the one of associated {@link BlockSequence}
     * @return the block provider
     */
    public BlockProvider getBlockProvider() {
        return super.getBlockSequence().getBlockProvider();
    }

    /**
     * Provides an associated entry (which contain the real business logic)
     * @return the entry
     */
    public T getEntry() {
        return fixSizeEntry;
    }

    /**
     * Provides a current header's size
     *
     * @return the size of the header
     */
    public int getHeaderSize() {
        throwExceptionIfInvalid("header size");
        if (isRoot()) {
            return getFirstHeaderSize();
        }
        return getRegularHeaderSize();
    }

    /**
     * Provides the size of all block's headers except of the first one
     * @return regular size of the header in bytes
     */
    public int getRegularHeaderSize() {
        return super.getHeaderSize() + bytesPerEntryNumber;
    }

    /**
     * Provides the header's size of the first block
     *
     * @return the size in bytes of the first block's header
     */
    public int getFirstHeaderSize() {
        return getRegularHeaderSize() + firstHeaderSize;
    }

    /**
     * Provides a total entry capacity of the block
     * @return the number of entries that can fit into one free block
     */
    public int getEntryCapacity() {
        throwExceptionIfInvalid("get capacity");
        return (getBlockSequence().getBlockProvider().getBlockContainer().getMedia().getMediaProperties().getBlockSize() - getHeaderSize())/fixSizeEntry.getEntrySize();
    }

    protected void throwExceptionIfInvalid(final String message) {
        if (invalid()) {
            throw new RoStoreException("The <" + this + "> is invalid: " + message);
        }
    }

    /**
     * Adds number of entries in this block
     *
     * @param number the number of entries to add (if positive) or remove (if negative)
     * @return the total number of entries added to the current block
     */
    public int addEntriesNumber(final int number) {
        throwExceptionIfInvalid("add number of entries");
        final Block block = getBlock();
        block.position(getBlockSequence().getBlockProvider().getBlockContainer().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        long oldNumber = block.getLong(bytesPerEntryNumber);
        long result = oldNumber + number;
        block.back(bytesPerEntryNumber);
        block.putLong(result, bytesPerEntryNumber);
        fixSizeEntry.validate();
        if (newEntryNumberListener!=null) {
            newEntryNumberListener.apply(result, number);
        }
        return (int)result;
    }

    /**
     * Provides a number of entries added to the block
     *
     * @return number of entries
     */
    public int getEntriesNumber() {
        throwExceptionIfInvalid("get number of entries");
        final Block block = getBlock();
        block.position(getBlockSequence().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        return (int)block.getLong(bytesPerEntryNumber);
    }

    /**
     * Provides an indication if the current block has free space
     *
     * @return {@code true} if there is a free space for at least one additional entry
     */
    public boolean hasFreeSpace() {
        throwExceptionIfInvalid("check free space");
        return getEntryCapacity() - getEntriesNumber() != 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createNewAfter() {
        throwExceptionIfInvalid("create new after");
        super.createNewAfter();
        fixSizeEntry.invalidate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isUnused() {
        return getEntriesNumber() == 0;
    }

    public void moveEntriesFrom(final int sourceIndex, final int sourceEntryStartIndex) {
        int thisIndex = getIndex();
        moveTo(sourceIndex);
        Block sourceBlock = getBlock();
        fixSizeEntry.moveTo(sourceEntryStartIndex);
        int sourcePosition = fixSizeEntry.getEntryLocation();
        int entriesToMove = getEntriesNumber() - sourceEntryStartIndex;
        int bytesToMove = fixSizeEntry.getEntrySize() * entriesToMove;
        moveTo(thisIndex);
        final Block targetBlock = getBlock();
        fixSizeEntry.last();
        int targetPosition;
        if (fixSizeEntry.getEntriesNumber() == 0) {
            targetPosition = getHeaderSize();
        } else {
            targetPosition = fixSizeEntry.getEntryLocation()+fixSizeEntry.getEntrySize();
        }
        targetBlock.position(targetPosition);
        sourceBlock.position(sourcePosition);
        targetBlock.put(sourceBlock, bytesToMove);
        addEntriesNumber(entriesToMove);
        moveTo(sourceIndex);
        addEntriesNumber(-entriesToMove);
    }

    public FixSizeEntryBlock(final BlockSequence<FixSizeEntryBlock> sequence,
                             final int firstHeaderSize,
                             final Function<FixSizeEntryBlock<T>, T> entryFactory,
                             final EntrySizeListener newEntryNumberListener) {
        super(sequence);
        this.firstHeaderSize = firstHeaderSize;
        fixSizeEntry = entryFactory.apply(this);
        this.newEntryNumberListener = newEntryNumberListener;
        int maxEntryNumber = (getBlockSequence().getBlockProvider().getMedia().getMediaProperties().getBlockSize()-getRegularHeaderSize()) / fixSizeEntry.getEntrySize();
        bytesPerEntryNumber = Utils.computeBytesForMaxValue(maxEntryNumber);
        root();
    }

    public boolean valid() {
        return super.valid();
    }

    public String toString() {
        String validPart;
        if (valid()) {
            Block block = super.getBlock();
            validPart = "#" + super.getIndex() + " " + block != null ? "block=" + block.toString():"no-block";
        } else {
            validPart = "invalid";
        }
        return "FixSizeEntryBlock: " + validPart;
    }

    public void invalidate() {
        super.invalidate();
        if (fixSizeEntry != null) {
            // in the constructor could be null
            fixSizeEntry.invalidate();
        }
    }

    public void moveTo(int seqIndex) {
        super.moveTo(seqIndex);
        if (fixSizeEntry != null) {
            // could be null in construction phase
            if (super.invalid()) {
                fixSizeEntry.invalidate();
            } else {
                fixSizeEntry.moveTo(0);
            }
        }
    }
}
