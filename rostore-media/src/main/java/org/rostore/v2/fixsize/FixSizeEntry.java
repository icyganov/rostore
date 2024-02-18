package org.rostore.v2.fixsize;

import org.rostore.v2.media.block.Block;
import org.rostore.entity.RoStoreException;

/**
 * Abstract entry that encapsulates operation on the entries with the fixed size.
 * <p>This object contains the reference of the block within the sequence,
 * so it specifies a specific entry within the sequence.</p>
 */
public abstract class FixSizeEntry extends ValidatingEntry {

    private FixSizeEntryBlock fixSizeEntryBlock;

    /**
     * {@inheritDoc}
     */
    @Override
    public int getEntriesNumber() {
        return fixSizeEntryBlock.getEntriesNumber();
    }

    /**
     * Returns a hash of the current location of the entry,
     * which encompasses with the index of the block and the index of the entry.
     * <p>Please note that the has can only be used if the block sequence is
     * not changed.</p>
     * <p>It also assumes that the number of blocks in the sequence and the number of entries
     * in each block is not bigger that the 32bit.</p>
     * @return the hash that identifies the position of the selected entry in the sequence
     */
    public long getHash() {
        long hash = fixSizeEntryBlock.getIndex();
        hash <<= 32;
        long indexLong = getIndex();
        indexLong &= 0xffffffff;
        hash |= indexLong;
        return hash;
    }

    /**
     * Moves the location of the current entry to the previously stored hash (see {@link #getHash()}).
     *
     * @param hash the hash to move to
     */
    public void moveToHash(final long hash) {
        if (hash == -1)  {
            fixSizeEntryBlock.invalidate();
        } else {
            int catalogBlockIndex = (int) ((hash >> 32) & 0xffffffff);
            int entryIndex = (int) (hash & 0xffffffff);
            fixSizeEntryBlock.moveTo(catalogBlockIndex);
            if (fixSizeEntryBlock.valid()) {
                moveTo(entryIndex);
            } else {
                invalidate();
            }
        }
    }

    /**
     * Provides an explicit string representation of the entry location
     */
    public String toString() {
        int oldPosition = getFixSizeEntryBlock().getBlock().position();
        try {
            return super.toString() + ", location=" + getEntryLocation() + ", " + fixSizeEntryBlock.toString();
        } finally {
            getFixSizeEntryBlock().getBlock().position(oldPosition);
        }
    }

    /**
     * Provides the block abstraction this entry is created with
     * @return the block
     */
    public FixSizeEntryBlock getFixSizeEntryBlock() {
        return fixSizeEntryBlock;
    }

    /**
     * Provides a pointer of the start of the entry within the block in bytes
     * <p>It validates if the entry is valid and throws an exception otherwise.</p>
     * @return the byte location of the entry within the body of the block
     */
    protected int getEntryLocation() {
        int entriesNumber = fixSizeEntryBlock.getEntriesNumber();
        int index = getIndex();
        if (index < 0 || index >= fixSizeEntryBlock.getEntriesNumber()) {
            throw new RoStoreException("The entry index " + index + " does not exist. max=" + entriesNumber);
        }
        return fixSizeEntryBlock.getHeaderSize() + index* getEntrySize();
    }

    /**
     * Creates an entry at the end of the current block.
     * <p>It will throw an exception in case the capacity of the block does not allow to expand it.</p>
     * <p>The number of entries will be increased.</p>
     * <p>The new index will be set to the last entry.</p>
     * <p>This operation will not validate the entry as it may be executed on the block without any entry (invalid one!)</p>
     */
    public void expand(){
        if (fixSizeEntryBlock.getEntriesNumber() >= fixSizeEntryBlock.getEntryCapacity()) {
            throw new RoStoreException("Can't add entry. Number of entries: " + fixSizeEntryBlock.getEntriesNumber() + ", capacity: " + fixSizeEntryBlock.getEntryCapacity());
        }
        int newSize = fixSizeEntryBlock.addEntriesNumber(1);
        moveTo(newSize - 1);
    }

    /**
     * Removes current entry and collapses the space it used.
     * <p>All the entries that follows the current one will be moved up by one entry size.</p>
     * <p>It will validate the current entry and throw an exception if the entry is invalid.</p>
     */
    public void remove() {
        throwExceptionIfInvalid("remove the entry");
        final Block block = fixSizeEntryBlock.getBlock();
        final int tailSize = (fixSizeEntryBlock.getEntriesNumber() - getIndex() - 1)*getEntrySize();
        block.position(getEntryLocation());
        block.collapseWindow(getEntrySize(), tailSize);
        fixSizeEntryBlock.addEntriesNumber(-1);
    }

    /**
     * Allocates an entry at the current index.
     * <p>All the entries at the current position to the end of the block are moved down the block body.</p>
     * <p>The function will validate if the entry location is valid and throw an exception otherwise.</p>
     * <p>It will not validate if the capacity is enough. Caller is responsible for it.</p>
     */
    public void insert() {
        final Block block = fixSizeEntryBlock.getBlock();
        final int entrySize = getEntrySize();
        final int tailSize = (fixSizeEntryBlock.getEntriesNumber() - getIndex())*entrySize;
        block.position(getEntryLocation());
        block.insertWindows(entrySize,tailSize);
        fixSizeEntryBlock.addEntriesNumber(1);
    }

    /**
     * Initializes the entry
     * @param fixSizeEntryBlock a block abstraction to be used for entries
     */
    public FixSizeEntry(final FixSizeEntryBlock fixSizeEntryBlock) {
        this.fixSizeEntryBlock = fixSizeEntryBlock;
        if (this.fixSizeEntryBlock.valid()) {
            if (this.fixSizeEntryBlock.getEntriesNumber() == 0) {
                invalidate();
            } else {
                moveTo(0);
            }
        } else {
            invalidate();
        }
    }

    /**
     * Provides an entry size in bytes
     * @return the entry size in bytes
     */
    public abstract int getEntrySize();
}
