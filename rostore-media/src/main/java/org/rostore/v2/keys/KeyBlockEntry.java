package org.rostore.v2.keys;

import org.rostore.Utils;
import org.rostore.entity.Record;
import org.rostore.v2.fixsize.FixSizeEntry;
import org.rostore.v2.fixsize.FixSizeEntryBlock;
import org.rostore.v2.media.block.Block;

public class KeyBlockEntry extends FixSizeEntry {

    private final VarSizeBlock varSizeBlock;
    private final VarSizeEntry varSizeEntry;
    private final VarSizeMultiBlock varSizeMultiBlock;
    private boolean initVarCall = false;
    private boolean sync = true;
    private final RecordLengths recordLengths;

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
                    if (blockIndex == getBlockIndex()) {
                        nextOffset = getBlockOffset();
                    }
                } else {
                    getFixSizeEntryBlock().next();
                    if (getFixSizeEntryBlock().valid()) {
                        if (blockIndex == getBlockIndex()) {
                            nextOffset = getBlockOffset();
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

    public long getBlockIndex() {
        throwExceptionIfInvalid("get block index");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation());
        return block.readBlockIndex();
    }

    public long getId() {
        throwExceptionIfInvalid("get id");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        return block.getLong(recordLengths.getIdLength());
    }

    public void setRecord(final Record record) {
        throwExceptionIfInvalid("set record");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() + getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        block.putLong(record.getId(), recordLengths.getIdLength());
        block.putLong(record.getEol(), recordLengths.getEolLength());
        block.putLong(record.getVersion(), recordLengths.getVersionLength());
    }

    public Record getRecord() {
        throwExceptionIfInvalid("get id and ttl and version");
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

    public boolean isExpired() {
        return isExpired(System.currentTimeMillis()/1000);
    }

    public boolean isExpired(long currentTimeSecs) {
        throwExceptionIfInvalid("is expired");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation() +
                getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() +
                getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset() +
                recordLengths.getIdLength());
        return Utils.isExpiredEOL(block.getLong(recordLengths.getEolLength()), currentTimeSecs);
    }

    public void setBlockIndex(long value) {
        throwExceptionIfInvalid("set block index");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation());
        block.writeBlockIndex(value);
    }

    public long getBlockOffset() {
        throwExceptionIfInvalid("get block offset");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation()+getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        return block.getLong(getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    public void setBlockOffset(long value) {
        throwExceptionIfInvalid("get block offset");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation()+getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        block.putLong(value, getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    public void incBlockOffset(long add) {
        throwExceptionIfInvalid("get block offset");
        final Block block = getFixSizeEntryBlock().getBlock();
        block.position(getEntryLocation()+getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        long value = block.getLong(getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        value += add;
        block.back(getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        block.putLong(value, getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    @Override
    public int getEntrySize() {
        return getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() +
                getFixSizeEntryBlock().getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset() +
                recordLengths.getTotalLength();
    }

    public VarSizeBlock getVarSizeBlock() {
        return varSizeBlock;
    }
}
