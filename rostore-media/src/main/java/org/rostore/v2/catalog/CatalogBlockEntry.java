package org.rostore.v2.catalog;

import org.rostore.v2.fixsize.FixSizeEntry;
import org.rostore.v2.media.block.Block;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.fixsize.FixSizeEntryBlock;

public class CatalogBlockEntry extends FixSizeEntry {

    public CatalogBlockEntry(final FixSizeEntryBlock<CatalogBlockEntry> freeBlock) {
        super(freeBlock);
    }

    public long getAddedNumber() {
        final FixSizeEntryBlock<CatalogBlockEntry> root = getFixSizeEntryBlock();
        if (!root.isRoot()) {
            throw new RoStoreException("This operation is only allowed on root");
        }
        final Block block = root.getBlock();
        block.position(root.getRegularHeaderSize());
        return block.readBlockIndex();
    }

    public void incAddedNumber(long added) {
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

    public long getBlocksNumber() {
        throwExceptionIfInvalid("get number of blocks");
        return getEntryStop()-getEntryStart()+1;
    }

    public void setEntryStart(long newStart) {
        throwExceptionIfInvalid("set entry start");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation());
        block.writeBlockIndex(newStart);
    }

    public void setEntryStartStop(final long newStart, final long newStop) {
        throwExceptionIfInvalid("set entry start/stop");
        int location = getEntryLocation();
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(location);
        block.writeBlockIndex(newStart);
        block.writeBlockIndex(newStop);
    }

    public long getEntryStart() {
        throwExceptionIfInvalid("get entry start");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation());
        return block.readBlockIndex();
    }

    public void setEntryStop(long newStop) {
        throwExceptionIfInvalid("set entry stop");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation()+ getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        block.writeBlockIndex(newStop);
    }

    public long getEntryStop() {
        throwExceptionIfInvalid("get entry stop");
        final Block block = getFixSizeEntryBlock().getBlock();
        final int bytesPerBlockIndex = getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
        block.position(getEntryLocation() + bytesPerBlockIndex);
        return block.readBlockIndex();
    }

    @Override
    public int getEntrySize() {
        return getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex()*2;
    }

}
