package org.rostore.v2.fixsize;

import org.rostore.Utils;
import org.rostore.v2.catalog.EntrySizeListener;
import org.rostore.v2.media.block.Block;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.seq.BlockSequence;
import org.rostore.v2.seq.SequenceBlock;

import java.util.function.Function;

public class FixSizeEntryBlock<T extends FixSizeEntry> extends SequenceBlock {

    private final T fixSizeEntry;
    private int firstHeaderSize;
    private int bytesPerEntryNumber;

    private final EntrySizeListener newEntryNumberListener;

    public BlockProvider getBlockProvider() {
        return super.getBlockSequence().getBlockProvider();
    }

    public T getEntry() {
        return fixSizeEntry;
    }

    public int getHeaderSize() {
        throwExceptionIfInvalid("header size");
        if (isRoot()) {
            return getFirstHeaderSize();
        }
        return getRegularHeaderSize();
    }

    public int getRegularHeaderSize() {
        return super.getHeaderSize() + bytesPerEntryNumber;
    }

    public int getFirstHeaderSize() {
        return getRegularHeaderSize() + firstHeaderSize;
    }

    /**
     * @return the number of entries fit into one free block
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

    public int addEntriesNumber(int number) {
        throwExceptionIfInvalid("dec number of entries");
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

    public int getEntriesNumber() {
        throwExceptionIfInvalid("get number of entries");
        final Block block = getBlock();
        block.position(getBlockSequence().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        return (int)block.getLong(bytesPerEntryNumber);
    }

    public boolean hasFreeSpace() {
        throwExceptionIfInvalid("check free space");
        return getEntryCapacity() - getEntriesNumber() != 0;
    }

    public void createNewAfter() {
        throwExceptionIfInvalid("create new after");
        super.createNewAfter();
        fixSizeEntry.invalidate();
    }

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

    public void moveTo(int index) {
        super.moveTo(index);
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
