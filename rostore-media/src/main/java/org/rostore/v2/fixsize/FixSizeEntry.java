package org.rostore.v2.fixsize;

import org.rostore.v2.media.block.Block;
import org.rostore.entity.RoStoreException;

public abstract class FixSizeEntry extends ValidatingEntry {

    private FixSizeEntryBlock fixSizeEntryBlock;

    @Override
    public int getEntriesNumber() {
        return fixSizeEntryBlock.getEntriesNumber();
    }

    public long getHash() {
        long hash = fixSizeEntryBlock.getIndex();
        hash <<= 32;
        long indexLong = getIndex();
        indexLong &= 0xffffffff;
        hash |= indexLong;
        return hash;
    }

    public void moveToHash(long hash) {
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

    public String toString() {
        int oldPosition = getFixSizeEntryBlock().getBlock().position();
        try {
            return super.toString() + ", location=" + getEntryLocation() + ", " + fixSizeEntryBlock.toString();
        } finally {
            getFixSizeEntryBlock().getBlock().position(oldPosition);
        }
    }

    public FixSizeEntryBlock getFixSizeEntryBlock() {
        return fixSizeEntryBlock;
    }

    protected int getEntryLocation() {
        int entriesNumber = fixSizeEntryBlock.getEntriesNumber();
        int index = getIndex();
        if (index < 0 || index >= fixSizeEntryBlock.getEntriesNumber()) {
            throw new RoStoreException("The entry index " + index + " does not exist. max=" + entriesNumber);
        }
        return fixSizeEntryBlock.getHeaderSize() + index* getEntrySize();
    }

    public void expand(){
        if (fixSizeEntryBlock.getEntriesNumber() >= fixSizeEntryBlock.getEntryCapacity()) {
            throw new RoStoreException("Can't add entry. Number of entries: " + fixSizeEntryBlock.getEntriesNumber() + ", capacity: " + fixSizeEntryBlock.getEntryCapacity());
        }
        int newSize = fixSizeEntryBlock.addEntriesNumber(1);
        moveTo(newSize - 1);
    }

    public void remove() {
        throwExceptionIfInvalid("remove the entry");
        final Block block = fixSizeEntryBlock.getBlock();
        final int tailSize = (fixSizeEntryBlock.getEntriesNumber() - getIndex() - 1)*getEntrySize();
        block.position(getEntryLocation());
        block.collapseWindow(getEntrySize(), tailSize);
        fixSizeEntryBlock.addEntriesNumber(-1);
    }

    /**
     * Allocates an entry at the current place
     */
    public void insert() {
        final Block block = fixSizeEntryBlock.getBlock();
        final int entrySize = getEntrySize();
        final int tailSize = (fixSizeEntryBlock.getEntriesNumber() - getIndex())*entrySize;
        block.position(getEntryLocation());
        block.insertWindows(entrySize,tailSize);
        fixSizeEntryBlock.addEntriesNumber(1);
    }

    public FixSizeEntry(final FixSizeEntryBlock freeBlock) {
        this.fixSizeEntryBlock = freeBlock;
        if (fixSizeEntryBlock.valid()) {
            if (fixSizeEntryBlock.getEntriesNumber() == 0) {
                invalidate();
            } else {
                moveTo(0);
            }
        } else {
            invalidate();
        }
    }

    public abstract int getEntrySize();
}
