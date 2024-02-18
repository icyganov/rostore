package org.rostore.v2.keys;

import org.rostore.Utils;
import org.rostore.entity.Record;
import org.rostore.v2.fixsize.FixSizeEntry;
import org.rostore.v2.fixsize.FixSizeEntryBlock;
import org.rostore.v2.media.block.Block;

/**
 * Represents an entry with exactly one key.
 * <p>Every entry contains: block index, block offset, id, eol, version.</p>
 * <p>Block index and offset reference a region in the var size block where the key data is stored.</p>
 * <p>Var size block can be either multi block or single block.</p>
 * <p>Multi block would reference a key, which size is greater than one block. A single (or multi-entry) block would reference
 * a block that contain one complete or several keys.</p>
 */
public class KeyBlockEntry extends FixSizeEntry {

    private final VarSizeBlock varSizeBlock;
    private final VarSizeEntry varSizeEntry;
    private final VarSizeMultiBlock varSizeMultiBlock;
    private boolean initVarCall = false;
    private boolean sync = true;
    private final RecordLengths recordLengths;

    /**
     * A class with length in bytes of key-entry elements
     * @return lengths of record's elements
     */
    public RecordLengths getRecordLengths() {
        return recordLengths;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder("KeyBlockEntry:");
        int oldPosition = getFixSizeEntryBlock().getBlock().position();
        try {
            if (invalid()) {
                sb.append(" invalid");
            } else {
                sb.append(" id ");
                sb.append(getId());
            }
            return sb.toString();
        } finally {
            getFixSizeEntryBlock().getBlock().position(oldPosition);
        }
    }

    /**
     * Manages if the var size block should be initialized when the entry is selected
     *
     * @param value {@code true} if the block should be initialized when the entry is changed
     * @return previous value of sync mode
     */
    public boolean sync(boolean value) {
        boolean before = this.sync;
        this.sync = value;
        if (valid() && sync) {
            initVarSize();
        }
        return before;
    }

    protected KeyBlockEntry(final FixSizeEntryBlock<KeyBlockEntry> keyBlock,
                            final VarSizeBlock varSizeBlock,
                            /** bytesPerId, EOL, Version*/
                            final RecordLengths recordLengths) {
        super(keyBlock);
        this.varSizeBlock = varSizeBlock;
        varSizeEntry = varSizeBlock.getEntry();
        varSizeMultiBlock = varSizeBlock.getMultiBlock();
        this.recordLengths = recordLengths;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveTo(int index) {
        super.moveTo(index);
        if (valid() && !initVarCall && sync) {
            initVarSize();
        }
    }

    /**
     * It resets the var-size blocks based on the current entry
     */
    private void initVarSize() {
        // this will break a recursion
        initVarCall = true;
        try {
            throwExceptionIfInvalid("get block index");
            final Block block = getFixSizeEntryBlock().getBlock();
            block.position(getEntryLocation());
            long blockIndex = block.readBlockIndex();
            int blockOffset = (int) block.getLong(getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
            varSizeBlock.moveTo(blockIndex);
            if (varSizeBlock.isMultiEntry()) {
                varSizeEntry.setOffset(blockOffset);
                long nextOffset = -1;
                long copy = getHash();
                next();
                if (valid()) {
                    if (blockIndex == getKeyBlockIndex()) {
                        nextOffset = getKeyBlockOffset();
                    }
                } else {
                    getFixSizeEntryBlock().next();
                    if (getFixSizeEntryBlock().valid()) {
                        if (blockIndex == getKeyBlockIndex()) {
                            nextOffset = getKeyBlockOffset();
                        }
                    }
                }
                moveToHash(copy);
                int size = -1;
                if (nextOffset != -1) {
                    size = (int) (nextOffset - blockOffset);
                }
                varSizeEntry.setEntrySize(size);
            } else {
                varSizeMultiBlock.root();
            }
        } finally {
            initVarCall = false;
        }
    }

    /**
     * Reads the key block index from the current entry
     *
     * @return key block index
     */
    public long getKeyBlockIndex() {
        throwExceptionIfInvalid("get block index");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation());
        return block.readBlockIndex();
    }

    /**
     * Reads an id associated with the current entry
     *
     * @return the id as it stored in the entry
     */
    public long getId() {
        throwExceptionIfInvalid("get id");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        return block.getLong(recordLengths.getIdLength());
    }

    /**
     * Put record to the current position / entry
     *
     * @param record the record to write to the key entry
     */
    public void setRecord(final Record record) {
        throwExceptionIfInvalid("set record");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        block.putLong(record.getId(), recordLengths.getIdLength());
        block.putLong(record.getEol(), recordLengths.getEolLength());
        block.putLong(record.getVersion(), recordLengths.getVersionLength());
    }

    /**
     * Reads the record from the current position
     *
     * @return the record object
     */
    public Record getRecord() {
        throwExceptionIfInvalid("get id, ttl and version");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        Record record = new Record();
        record.id(block.getLong(recordLengths.getIdLength()));
        if (recordLengths.getEolLength() != 0) {
            record.eol(block.getLong(recordLengths.getEolLength()));
        }
        if (recordLengths.getVersionLength() != 0) {
            record.version(block.getLong(recordLengths.getVersionLength()));
        }
        return record;
    }

    /**
     * Checks if current entry is expired (according to EOL)
     *
     * @return {@code true} if the entry already expired
     */
    public boolean isExpired() {
        return isExpired(System.currentTimeMillis()/1000);
    }

    /**
     * Checks if current entry is expired (according to EOL)
     *
     * @param currentTimeSecs current timestamp in seconds
     * @return {@code true} if the entry expired according to the given timestamp
     */
    public boolean isExpired(long currentTimeSecs) {
        throwExceptionIfInvalid("is expired");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation() +
                getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() +
                getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset() +
                recordLengths.getIdLength());
        return Utils.isExpiredEOL(block.getLong(recordLengths.getEolLength()), currentTimeSecs);
    }

    /**
     * Sets the key block to the provided index
     *
     * @param blockIndex the block index
     */
    public void setKeyBlockIndex(final long blockIndex) {
        throwExceptionIfInvalid("set block index");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation());
        block.writeBlockIndex(blockIndex);
    }

    /**
     * Provides an offset of the key in the key block
     *
     * @return the offset within the key block
     */
    public long getKeyBlockOffset() {
        throwExceptionIfInvalid("get block offset");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation()+getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        return block.getLong(getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    /**
     * Sets offset of the key in the key block
     *
     * @param offset the offset to associate with the key
     */
    public void setKeyBlockOffset(long offset) {
        throwExceptionIfInvalid("get block offset");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation()+getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        block.putLong(offset, getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    /**
     * Increments the offset associated with the key by provided increment
     *
     * @param add how much to increment
     */
    public void incKeyBlockOffset(final long add) {
        throwExceptionIfInvalid("get block offset");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation()+getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        long value = block.getLong(getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        value += add;
        block.back(getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        block.putLong(value, getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getEntrySize() {
        return getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() +
                getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset() +
                recordLengths.getTotalLength();
    }

}
