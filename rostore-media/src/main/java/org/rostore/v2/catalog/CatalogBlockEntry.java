package org.rostore.v2.catalog;

import org.rostore.v2.fixsize.FixSizeEntry;
import org.rostore.v2.media.block.Block;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.fixsize.FixSizeEntryBlock;

/**
 * An entry used to store the elements of catalog.
 * <p>The entries in the catalog are stored sorted.</p>
 * <p>Every entry contains two elements: start and stop block index.</p>
 * <p>Catalog stores a total number of blocks stored in the catalog in the header of first block. This is needed to be able to fast
 * answer the request to know how many blocks are under control of catalog.</p>
 */
public class CatalogBlockEntry extends FixSizeEntry {

    public CatalogBlockEntry(final FixSizeEntryBlock<CatalogBlockEntry> freeBlock) {
        super(freeBlock);
    }

    /**
     * Provides a total number of blocks added to the catalog, stored in the first block of the sequence.
     * <p>Should only be called on the root block.</p>
     *
     * @return the total number of blocks in the catalog
     */
    public long getAddedNumber() {
        final FixSizeEntryBlock<CatalogBlockEntry> rootBlock = getFixSizeEntryBlock();
        if (!rootBlock.isRoot()) {
            throw new RoStoreException("This operation is only allowed on root");
        }
        final Block block = rootBlock.getBlock();
        block.position(rootBlock.getRegularHeaderSize());
        return block.readBlockIndex();
    }

    /**
     * Increments the total number of blocks added to the catalog, stored in the first block of the sequence.
     * <p>Should only be called on the root block.</p>
     *
     * @param added number of blocks to add (if positive), or remove (if negative)
     */
    public void incAddedNumber(final long added) {
        final FixSizeEntryBlock<CatalogBlockEntry> root = getFixSizeEntryBlock();
        final Block block = root.getBlock();
        // alloc number
        long number = getAddedNumber() + added;
        block.backBlockIndex();
        block.writeBlockIndex(number);
    }

    public void addNewEntryFor(final long startIndex,  final long stopIndex) {
        expand();
        setEntryStartStop(startIndex, stopIndex);
    }

    /**
     * Get the number of blocks in the current entry
     *
     * @return number of blocks in the current entry
     */
    public long getBlocksNumber() {
        throwExceptionIfInvalid("get number of blocks");
        return getEntryStop()-getEntryStart()+1;
    }

    /**
     * Sets the start block index in the current entry
     *
     * @param newStart the block index of the entry's start
     */
    public void setEntryStart(long newStart) {
        throwExceptionIfInvalid("set entry start");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation());
        block.writeBlockIndex(newStart);
    }

    /**
     * Sets both start and stop block indices in the current entry
     *
     * @param newStart the block index of the start block
     * @param newStop the block index of the stop block
     */
    public void setEntryStartStop(final long newStart, final long newStop) {
        throwExceptionIfInvalid("set entry start/stop");
        int location = getEntryLocation();
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(location);
        block.writeBlockIndex(newStart);
        block.writeBlockIndex(newStop);
    }

    /**
     * Provides the start block in the entry
     *
     * @return the start block index
     */
    public long getEntryStart() {
        throwExceptionIfInvalid("get entry start");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation());
        return block.readBlockIndex();
    }

    /**
     * Sets the stop block in the entry
     *
     * @param newStop the stop block index
     */
    public void setEntryStop(final long newStop) {
        throwExceptionIfInvalid("set entry stop");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation()+ getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        block.writeBlockIndex(newStop);
    }

    /**
     * Provides the stop block in the entry
     *
     * @return the stop block index
     */
    public long getEntryStop() {
        throwExceptionIfInvalid("get entry stop");
        final Block block = getFixSizeEntryBlock().getBlock();
        final int bytesPerBlockIndex = getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
        block.position(getEntryLocation() + bytesPerBlockIndex);
        return block.readBlockIndex();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getEntrySize() {
        return getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex()*2;
    }

}
