package org.rostore.v2.catalog;

import org.rostore.v2.media.block.Block;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.fixsize.FixSizeEntryBlock;

public class CatalogBlockEntry extends CatalogBlockEntryBase {

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

}
