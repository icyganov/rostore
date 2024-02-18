package org.rostore.v2.keys;

import org.rostore.Utils;
import org.rostore.entity.*;
import org.rostore.entity.Record;
import org.rostore.entity.media.RecordOption;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.fixsize.FixSizeEntryBlock;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.InternalBlockProvider;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.block.container.Status;
import org.rostore.v2.seq.BlockSequence;
import org.rostore.v2.seq.SequenceBlock;

import java.util.function.Function;

public class KeyBlockOperations implements Committable {

    private final FixSizeEntryBlock<KeyBlockEntry> keyBlock;
    private final VarSizeBlock varSizeBlock;
    private final VarSizeMultiBlock varSizeMultiBlock;
    private final VarSizeEntry varSizeEntry;
    private final KeyBlockEntry keyBlockEntry;

    private boolean rebalance = false;

    public BlockSequence getBlockSequence() {
        return keyBlock.getBlockSequence();
    }

    /**
     * Function calculates how many blocks is used by this key block area
     */
    public void remove() {
        CatalogBlockIndices toFree = new CatalogBlockIndices();
        for(int i=0; i<keyBlock.getBlockSequence().length(); i++) {
            keyBlock.moveTo(i);
            for(int j=0; j<keyBlock.getEntriesNumber(); j++) {
                keyBlockEntry.moveTo(j);
                if (varSizeBlock.isMultiBlock()) {
                    varSizeMultiBlock.free();
                } else {
                    if (!toFree.contains(keyBlockEntry.getKeyBlockIndex())) {
                        toFree.add(keyBlockEntry.getKeyBlockIndex(),keyBlockEntry.getKeyBlockIndex());
                    }
                }
            }
        }
        varSizeBlock.getBlockProvider().getBlockAllocator().free(toFree);
        varSizeBlock.getBlockProvider().getBlockAllocator().free(keyBlock.getBlockSequence().getBlockIndexSequence().createCatalogBlockIndices());
    }

    public void dump() {
        for(int i=0; i<keyBlock.getBlockSequence().length(); i++) {
            keyBlock.moveTo(i);
            System.out.println(keyBlock.getBlock().getAbsoluteIndex() + ": H" + keyBlock.getHeaderSize() + "E"  + keyBlockEntry.getEntrySize() + "x" + + keyBlock.getEntriesNumber() );
            for(int j=0; j<keyBlock.getEntriesNumber(); j++) {
                keyBlockEntry.moveTo(j);
                System.out.println(" " + keyBlockEntry + " --> " + varSizeBlock.toString());
            }
        }
    }

    public static KeyBlockOperations load(final BlockAllocator blockAllocator,
                                          final long startIndex,
                                          final RecordLengths recordLengths) {
        final BlockProvider blockProvider = InternalBlockProvider.create(blockAllocator);
        return new KeyBlockOperations(
                blockProvider,
                kbo -> SequenceBlock.load(blockProvider, startIndex,
                (Function<BlockSequence<FixSizeEntryBlock>, FixSizeEntryBlock>) sequence ->
                        new FixSizeEntryBlock(sequence,
                                0,
                                (Function<FixSizeEntryBlock<KeyBlockEntry>, KeyBlockEntry>)b -> new KeyBlockEntry(b, kbo.varSizeBlock, recordLengths),
                                null), BlockType.KEY));
    }

    public static KeyBlockOperations create(final BlockAllocator blockAllocator,
                                            final RecordLengths recordLengths) {
        final BlockProvider blockProvider = InternalBlockProvider.create(blockAllocator);
        return new KeyBlockOperations(
                blockProvider,
                kbo -> SequenceBlock.create(blockProvider,
                (Function<BlockSequence<FixSizeEntryBlock>, FixSizeEntryBlock>) sequence ->
                        new FixSizeEntryBlock(sequence,
                                0,
                                (Function<FixSizeEntryBlock<KeyBlockEntry>, KeyBlockEntry>)b -> new KeyBlockEntry(b, kbo.varSizeBlock, recordLengths),
                                null), BlockType.KEY));
    }

    public long getStartIndex() {
        return keyBlock.getBlockSequence().getBlockIndexSequence().getBlockIndex(0);
    }

    private KeyBlockOperations(final BlockProvider blockProvider,
                               final Function<KeyBlockOperations, FixSizeEntryBlock<KeyBlockEntry>> keyBlockFactory) {
        varSizeBlock = new VarSizeBlock(blockProvider);
        varSizeEntry = varSizeBlock.getEntry();
        varSizeMultiBlock = varSizeBlock.getMultiBlock();
        keyBlock = keyBlockFactory.apply(this);
        keyBlockEntry = keyBlock.getEntry();
    }

    /**
     * This function is to look up the expired entries and
     * remove them
     *
     * @param blockIndex the key block to lookup in
     * @return the id of the removed expired entry or {@link Utils#ID_UNDEFINED} if nothing has been deleted
     */
    public long removeIfExpired(final int blockIndex) {
        try {
            keyBlock.moveTo(blockIndex);
            if (keyBlock.invalid()) {
                return Utils.ID_UNDEFINED;
            }
            keyBlockEntry.sync(false);
            long currentTimeSecs = System.currentTimeMillis() / 1000;
            try {
                keyBlockEntry.first();
                while (keyBlockEntry.valid() && !keyBlockEntry.isExpired(currentTimeSecs)) {
                    keyBlockEntry.next();
                }
            } finally {
                keyBlockEntry.sync(true);
            }
            if (keyBlockEntry.valid() && keyBlockEntry.isExpired(currentTimeSecs)) {
                long id = keyBlockEntry.getId();
                removeEntryInternally();
                return id;
            }
            return Utils.ID_UNDEFINED;
        } finally {
            rebalance();
        }
    }

    /**
     * throws {@link VersionMismatchException}
     * @param key
     * @return
     */
    public boolean remove(final byte[] key, final Record record) {
        try {
            keyBlock.root();
            int cmp;
            if (keyBlockEntry.valid()) {
                cmp = varSizeBlock.compare(key);
                if (cmp < 0) {
                    return false;
                }
                if (cmp == 0) {
                    boolean expired = keyBlockEntry.isExpired();
                    removeEntry(record);
                    return !expired;
                }
            } else {
                // root is invalid
                keyBlock.next();
                if (keyBlock.invalid()) {
                    return false;
                }
                cmp = varSizeBlock.compare(key);
                if (cmp == 0) {
                    boolean expired = keyBlockEntry.isExpired();
                    removeEntry(record);
                    return !expired;
                }
                if (cmp < 0) {
                    return false;
                }
            }
            keyBlock.last();
            keyBlockEntry.last();
            cmp = varSizeBlock.compare(key);
            if (cmp == 0) {
                boolean expired = keyBlockEntry.isExpired();
                removeEntry(record);
                return !expired;
            }
            if (cmp > 0) {
                return false;
            }
            cmp = findAfter(key);
            if (cmp == 0) {
                boolean expired = keyBlockEntry.isExpired();
                removeEntry(record);
                return !expired;
            }
            return false;
        } finally {
            rebalance();
        }
    }

    void removeEntry(final Record record) {
        final Record recordToRemove = keyBlockEntry.getRecord();
        if (!keyBlockEntry.isExpired()) {
            VersionMismatchException.checkAndThrow(recordToRemove.getVersion(), record.getVersion(), record.hasOption(RecordOption.OVERRIDE_VERSION));
        }
        removeEntryInternally();
        record.eol(recordToRemove.getEol());
        record.id(recordToRemove.getId());
    }

    void removeEntryInternally() {
        if (varSizeBlock.isMultiBlock()) {
            varSizeMultiBlock.free();
        } else {
            // this is multi entry
            if (varSizeEntry.getDataLength() == varSizeEntry.getEntrySize()) {
                // this is the last entry
                varSizeBlock.getBlockProvider().freeBlock(varSizeBlock.getBlock().getAbsoluteIndex());
            } else {
                int size = varSizeEntry.getEntrySize();
                varSizeEntry.remove();
                long hash = keyBlockEntry.getHash();
                correctAfterInsert(-size);
                keyBlockEntry.moveToHash(hash);
            }
        }
        removeKeyEntry();
    }

    private void removeKeyEntry() {
        if (keyBlock.getEntriesNumber() == 1) {
            // removing the last entry
            if (keyBlock.isRoot()) {
                keyBlockEntry.remove();
            } else {
                keyBlock.delete();
                markToRebalance();
            }
        } else {
            keyBlockEntry.remove();
        }
    }

    private void markToRebalance() {
        rebalance = true;
    }

    private void rebalance() {
        if (rebalance) {
            keyBlock.getBlockSequence().rebalance();
            rebalance = false;
        }
    }


    /**
     * Store the record for specified key
     *
     * @param key the key to store
     * @param record the record to store
     * @return the previous id (if one exists) or {@link Utils#ID_UNDEFINED}
     */
    public long put(final byte[] key, final Record record) {
        try {
            keyBlock.root();
            int cmp;
            if (keyBlockEntry.valid()) {
                cmp = varSizeBlock.compare(key);
                if (cmp < 0) {
                    OptionMismatchException.checkInsertRecord(record);
                    VersionMismatchInitException.checkAndThrow(record);
                    insertFirstEntry(key, record);
                    return Utils.ID_UNDEFINED;
                }
                if (cmp == 0) {
                    return updateRecord(record);
                }
            } else {
                // root is invalid
                keyBlock.next();
                if (keyBlock.invalid()) {
                    // next after root is also invalid => add the first entry
                    keyBlock.root();
                    OptionMismatchException.checkInsertRecord(record);
                    VersionMismatchInitException.checkAndThrow(record);
                    insertFirstEntry(key, record);
                    return Utils.ID_UNDEFINED;
                } else {
                    // it all starts here
                    cmp = varSizeBlock.compare(key);
                    if (cmp < 0) {
                        keyBlock.root();
                        OptionMismatchException.checkInsertRecord(record);
                        VersionMismatchInitException.checkAndThrow(record);
                        insertFirstEntry(key, record);
                        return Utils.ID_UNDEFINED;
                    }
                    if (cmp == 0) {
                        return updateRecord(record);
                    }
                }
            }
            keyBlock.last();
            keyBlockEntry.last();
            cmp = varSizeBlock.compare(key);
            if (cmp == 0) {
                return updateRecord(record);
            }
            if (cmp > 0) {
                OptionMismatchException.checkInsertRecord(record);
                VersionMismatchInitException.checkAndThrow(record);
                expandLastEntry(key, record);
                return Utils.ID_UNDEFINED;
            }
            cmp = findAfter(key);
            if (cmp == 0) {
                return updateRecord(record);
            }
            OptionMismatchException.checkInsertRecord(record);
            VersionMismatchInitException.checkAndThrow(record);
            insertBeforeEntry(key, record);
            return Utils.ID_UNDEFINED;
        } finally {
            rebalance();
        }
    }

    /**
     * Sets the parameters from the record to the current entry
     *
     * <p>The validation of the update will also be executed.</p>
     * <p>If the version would not fit or any option would prevent the operation
     * to be executed, a fitting exception will be thrown,</p>
     *
     * @param record the new values to store
     * @return the previous value of the id associated with the record
     */
    private long updateRecord(final Record record) {
        final Record previousRecord = keyBlockEntry.getRecord();
        if (keyBlockEntry.isExpired()) {
            OptionMismatchException.checkInsertRecord(record);
        } else {
            OptionMismatchException.checkUpdateRecord(keyBlockEntry, record);
        }
        if (!record.hasOption(RecordOption.OVERRIDE_VERSION)) {
            if (keyBlockEntry.isExpired()) {
                VersionMismatchInitException.checkAndThrow(record);
                record.incrementVersion(keyBlockEntry.getRecordLengths().getVersionLength());
            } else {
                VersionMismatchException.checkAndThrow(previousRecord.getVersion(), record);
                record.version(previousRecord.getVersion());
                record.incrementVersion(keyBlockEntry.getRecordLengths().getVersionLength());
            }
        }
        keyBlockEntry.setRecord(record);
        return previousRecord.getId();
    }

    private void insertFirstEntry(final byte[] key, final Record record) {
        if (shouldBeMultiBlock(key)) {
            long blockIndex = varSizeMultiBlock.put(key);
            insertKeyEntry(blockIndex, 0, record);
        } else {
            if (keyBlockEntry.invalid()) {
                // nothing is there
                insertSingleEntryBlock(key, record);
            } else {
                if (varSizeBlock.isMultiBlock()) {
                    insertSingleEntryBlock(key, record);
                } else {
                    if (varSizeEntry.getFreeSpace() > key.length) {
                        varSizeEntry.insert(key);
                        insertKeyEntry(varSizeBlock.getBlockIndex(), varSizeEntry.getOffset(), record);
                        correctAfterInsert(key.length);
                    } else {
                        insertSingleEntryBlock(key, record);
                    }
                }
            }

        }
    }

    private void expandLastEntry(final byte[] key, final Record record) {
        if (shouldBeMultiBlock(key)) {
            long blockIndex = varSizeMultiBlock.put(key);
            expandKeyEntry(blockIndex, 0, record);
        } else {
            if (varSizeBlock.isMultiEntry()) {
                // we are inserting at the end of multi entry
                if (varSizeEntry.getFreeSpace() > key.length) {
                    varSizeEntry.expand(key);
                    expandKeyEntry(varSizeBlock.getBlockIndex(), varSizeEntry.getOffset(), record);
                } else {
                    expandSingleEntryBlock(key, record);
                }
            } else {
                // the latest is a multi block
                expandSingleEntryBlock(key, record);
            }
        }
    }

    private boolean shouldBeMultiBlock(final byte[] key) {
        // it should be a multiblock if it is impossible to put 2 entries in the entryblock
        // it is important rule, otherwise the entry division logic will fail
        int multiEntryCapacity = varSizeBlock.getBlockProvider().getBlockAllocator().getMedia().getMediaProperties().getBlockSize() - varSizeBlock.getMultiEntryHeaderSize();
        return key.length > multiEntryCapacity / 2;
    }

    private void insertBeforeEntry(final byte[] key, final Record record) {
        if (shouldBeMultiBlock(key)) {
            // it is going to be a multi block
            long blockIndex = varSizeMultiBlock.put(key);
            if (varSizeBlock.isMultiBlock()) {
                // this also was a multiblock
                insertKeyEntry(blockIndex, 0, record);
            } else {
                // this was an entry block
                if (varSizeEntry.isFirst()) {
                    // we are inserting before the first entry
                    insertKeyEntry(blockIndex, 0, record);
                } else {
                    long hash = keyBlockEntry.getHash();
                    final Block nextBlock = varSizeBlock.getBlockProvider().allocateBlock(BlockType.KEY);
                    varSizeEntry.split(nextBlock);
                    correctAfterMove(nextBlock.getAbsoluteIndex(),0);
                    keyBlockEntry.moveToHash(hash);
                    insertKeyEntry(blockIndex,varSizeBlock.getMultiEntryHeaderSize(), record);
                }
            }
        } else {
            // it is going to be an entry
            if (varSizeBlock.isMultiBlock()) {
                // current is a multiblock
                long afterHash = keyBlockEntry.getHash();
                previousEntry();
                if (varSizeBlock.isMultiEntry()) {
                    // the previous was a multi entry
                    if (varSizeEntry.getFreeSpace() >= key.length ) {
                        // we have enough space. Extend
                        varSizeEntry.expand(key);
                        long blockIndex = varSizeBlock.getBlockIndex();
                        long offset = varSizeEntry.getOffset();
                        keyBlockEntry.moveToHash(afterHash);
                        insertKeyEntry(blockIndex, offset, record);
                    } else {
                        keyBlockEntry.moveToHash(afterHash);
                        insertSingleEntryBlock(key, record);
                    }
                } else {
                    // the previous was also multi block
                    keyBlockEntry.moveToHash(afterHash);
                    insertSingleEntryBlock(key, record);
                }
            } else {
                // this is was an entry
                if (varSizeEntry.getFreeSpace() >= key.length) {
                    // we can put the entry to this entry
                    int offset = varSizeEntry.getOffset();
                    varSizeEntry.insert(key);
                    insertKeyEntry(varSizeBlock.getBlockIndex(), offset, record);
                    correctAfterInsert(key.length);
                } else {
                    // not enough space
                    long hash = keyBlockEntry.getHash();
                    if (varSizeEntry.isFirst()) {
                        previousEntry();
                        if (keyBlockEntry.valid()) {
                            if (varSizeBlock.isMultiEntry() && varSizeEntry.getFreeSpace() >= key.length) {
                                // we have a previous entry with enough space
                                varSizeEntry.expand(key);
                                long blockIndex = varSizeBlock.getBlock().getAbsoluteIndex();
                                long offset = varSizeEntry.getOffset();
                                keyBlockEntry.moveToHash(hash);
                                insertKeyEntry(blockIndex, offset, record);
                                return;
                            } else {
                                final Block betweenBlock = varSizeBlock.getBlockProvider().allocateBlock(BlockType.KEY);
                                varSizeEntry.init(betweenBlock);
                                varSizeBlock.moveTo(betweenBlock.getAbsoluteIndex());
                                varSizeEntry.expand(key);
                                keyBlockEntry.moveToHash(hash);
                                insertKeyEntry(betweenBlock.getAbsoluteIndex(), varSizeEntry.getOffset(), record);
                                return;
                            }
                        }
                    }
                    keyBlockEntry.moveToHash(hash);
                    long offset = keyBlockEntry.getKeyBlockOffset();
                    long blockIndex = keyBlockEntry.getKeyBlockIndex();
                    final Block nextBlock = varSizeBlock.getBlockProvider().allocateBlock(BlockType.KEY);
                    long spaceBeFreed = varSizeBlock.getBlockProvider().getBlockContainer().getMedia().getMediaProperties().getBlockSize() - offset;
                    if (spaceBeFreed > key.length) {
                        varSizeEntry.split(nextBlock);
                        varSizeEntry.expand(key);
                        correctAfterMove(nextBlock.getAbsoluteIndex(), 0);
                        keyBlockEntry.moveToHash(hash);
                        insertKeyEntry(blockIndex, offset, record);
                    } else {
                        varSizeEntry.split(nextBlock, key);
                        correctAfterMove(nextBlock.getAbsoluteIndex(), key.length);
                        keyBlockEntry.moveToHash(hash);
                        insertKeyEntry(nextBlock.getAbsoluteIndex(), varSizeBlock.getMultiEntryHeaderSize(), record);
                    }
                }
            }
        }
    }

    private void previousEntry() {
        keyBlockEntry.previous();
        if (keyBlockEntry.invalid()) {
            keyBlock.previous();
            keyBlockEntry.last();
        }
    }

    private void fillNewKeyEntry(final long blockIndex, final long blockOffset, final Record record) {
        keyBlockEntry.setKeyBlockIndex(blockIndex);
        keyBlockEntry.setKeyBlockOffset(blockOffset);
        if (!record.hasOption(RecordOption.OVERRIDE_VERSION)) {
            record.incrementVersion(keyBlockEntry.getRecordLengths().getVersionLength());
        }
        keyBlockEntry.setRecord(record);
    }

    private void insertSingleEntryBlock(byte[] key, Record record) {
        final Block newBlock = varSizeBlock.getBlockProvider().allocateBlock(BlockType.KEY);
        if (!varSizeEntry.isLast()) {
            varSizeBlock.moveTo(newBlock.getAbsoluteIndex());
        }
        varSizeEntry.init(newBlock);
        insertKeyEntry(newBlock.getAbsoluteIndex(), varSizeBlock.getMultiEntryHeaderSize(), record);
        varSizeEntry.expand(key);
    }

    private void expandSingleEntryBlock(final byte[] key, final Record record) {
        Block newBlock = varSizeBlock.getBlockProvider().allocateBlock(BlockType.KEY);
        varSizeEntry.init(newBlock);
        expandKeyEntry(newBlock.getAbsoluteIndex(), varSizeBlock.getMultiEntryHeaderSize(), record);
        // the ext will shift the varSizeEntry to the newly added point
        keyBlockEntry.sync(true);
        varSizeEntry.expand(key);
    }
    
    private void correctAfterMove(long newBlockIndex, long newLocationOffset) {
        long correction = -varSizeEntry.getOffset()+varSizeBlock.getMultiEntryHeaderSize() + newLocationOffset;
        long oldBlockIndex = keyBlockEntry.getKeyBlockIndex();
        while(keyBlockEntry.valid() && keyBlockEntry.getKeyBlockIndex() == oldBlockIndex) {
            keyBlockEntry.setKeyBlockIndex(newBlockIndex);
            keyBlockEntry.incKeyBlockOffset(correction);
            nextKeyEntry();
        }
    }

    private void correctAfterInsert(final long size) {
        long blockIndex = keyBlockEntry.getKeyBlockIndex();
        nextKeyEntry();
        while(keyBlockEntry.valid() && keyBlockEntry.getKeyBlockIndex() == blockIndex) {
            keyBlockEntry.incKeyBlockOffset(size);
            nextKeyEntry();
        }
    }
    
    private void nextKeyEntry() {
        keyBlockEntry.next();
        if (keyBlockEntry.invalid()) {
            keyBlock.next();
        }
    }

    private void expandKeyEntry(long blockIndex, long offset, Record record) {
        boolean syncBefore = keyBlockEntry.sync(false);
        try {
            if (keyBlock.hasFreeSpace()) {
                keyBlockEntry.expand();
            } else {
                keyBlock.createNewAfter();
                keyBlockEntry.expand();
                markToRebalance();
            }
            fillNewKeyEntry(blockIndex, offset, record);
        } finally {
            keyBlockEntry.sync(syncBefore);
        }
    }

    private void insertKeyEntry(final long blockIndex, final long offset, final Record record) {
        boolean syncBefore = keyBlockEntry.sync(false);
        try {
            if (keyBlock.hasFreeSpace()) {
                if (keyBlockEntry.valid()) {
                    keyBlockEntry.insert();
                } else {
                    keyBlockEntry.expand();
                }
            } else {
                if (keyBlockEntry.isFirst() && !keyBlock.isRoot()) {
                    // no-space and we are the first entry, but not a root
                    long hash = keyBlockEntry.getHash();
                    keyBlock.previous();
                    if (keyBlock.hasFreeSpace()) {
                        keyBlockEntry.expand();
                        fillNewKeyEntry(blockIndex, offset, record);
                        return;
                    }
                    keyBlockEntry.moveToHash(hash);
                }
                int moveStartIndex = keyBlockEntry.getIndex();
                int beforeBlockIndex = keyBlock.getIndex();
                keyBlock.createNewAfter();
                markToRebalance();
                keyBlock.moveEntriesFrom(beforeBlockIndex, moveStartIndex);
                keyBlock.moveTo(beforeBlockIndex);
                keyBlockEntry.expand();
            }
            fillNewKeyEntry(blockIndex, offset, record);
        } finally {
            keyBlockEntry.sync(syncBefore);
        }
    }

    private int findAfter(final byte[] key) {
        keyBlock.root();
        if (keyBlock.getEntriesNumber()==0) {
            // start with next if root is empty
            keyBlock.next();
        }
        if (keyBlock.invalid()) {
            return -1;
        }
        keyBlockEntry.last();
        int startIndex = keyBlock.getIndex();
        // but should be at least the last one
        int stopIndex = keyBlock.getBlockSequence().length() - 1;

        int cmp = varSizeBlock.compare(key);
        if (cmp == 0) {
            return 0;
        }
        if (cmp < 0) {
            return searchInBlock(key);
        }
        if (startIndex == stopIndex) {
            return 1;
        }
        while (stopIndex - startIndex != 1) {
            int next = (startIndex + stopIndex) / 2;
            keyBlock.moveTo(next);
            keyBlockEntry.last();
            cmp = varSizeBlock.compare(key);
            if (cmp == 0) {
                return 0;
            }
            if (cmp < 0) {
                stopIndex = next;
            } else {
                startIndex = next;
            }
        }
        keyBlock.moveTo(stopIndex);
        return searchInBlock(key);
    }

    private int searchInBlock(final byte[] key) {
        // search a point somewhere in the selected block
        keyBlockEntry.first();
        int cmp = varSizeBlock.compare(key);
        if (cmp == 0) {
            return 0;
        }
        if (cmp < 0) {
            return -1;
        }
        int startIndex = 0;
        int stopIndex = keyBlockEntry.getEntriesNumber() - 1;
        if (stopIndex == startIndex) {
            return 1;
        }
        while (stopIndex - startIndex != 1) {
            int next = (startIndex + stopIndex) / 2;
            keyBlockEntry.moveTo(next);
            cmp = varSizeBlock.compare(key);
            if (cmp == 0) {
                return 0;
            }
            if (cmp < 0) {
                stopIndex = next;
            } else {
                startIndex = next;
            }
        }
        keyBlockEntry.moveTo(stopIndex);
        return varSizeBlock.compare(key);
    }

    public KeyList list(byte[] startWithKey, byte[] continuationKey, long maxNumber, long maxSize) {
        KeyList keyList = new KeyList();
        if (continuationKey != null) {
            int cmp = findAfter(continuationKey);
            if (cmp == 0) {
                keyBlockEntry.next();
                if (keyBlockEntry.invalid()) {
                    keyBlock.next();
                    if (keyBlock.invalid()) {
                        return keyList;
                    }
                    keyBlockEntry.first();
                }
            }
        } else {
            // no continuation Key, so starts a new
            if (startWithKey != null && startWithKey.length != 0) {
                findAfter(startWithKey);
            } else {
                // no startsWithKey
                keyBlock.root();
                if (keyBlockEntry.getEntriesNumber() == 0) {
                    keyBlock.next();
                }
                if (keyBlock.invalid()) {
                    return keyList;
                }
                keyBlockEntry.first();
            }
        }
        if (keyBlockEntry.invalid()) {
            return keyList;
        }
        long timeSec = System.currentTimeMillis()/1000;
        while(true) {
            if (!keyBlockEntry.isExpired(timeSec)) {
                final byte[] key = varSizeBlock.get();
                if (startWithKey != null) {
                    if (key.length < startWithKey.length) {
                        return keyList;
                    } else {
                        for(int i=startWithKey.length-1; i>=0; i--) {
                            if (startWithKey[i] != key[i]) {
                                return keyList;
                            }
                        }
                    }
                }
                keyList.addKey(key);
            }
            keyBlockEntry.next();
            if (keyBlockEntry.invalid()) {
                keyBlock.next();
                if (keyBlock.invalid()) {
                    return keyList;
                }
                keyBlockEntry.first();
                if (keyBlockEntry.invalid()) {
                    return keyList;
                }
            }
            if (keyList.getSize() >= maxSize || keyList.getKeys().size() >= maxNumber) {
                keyList.setMore(true);
                return keyList;
            }
        }
    }

    /**
     * Searches for the provided key and return the {@link Record}
     * associated with it.
     *
     * @param key a key to return
     * @return the record if one has been found or {@code null}
     */
    public Record getRecord(final byte[] key) {
        int cmp = findAfter(key);
        if (cmp == 0) {
            if (keyBlockEntry.isExpired()) {
                return null;
            }
            return keyBlockEntry.getRecord();
        }
        return null;
    }

    @Override
    public void close() {
        keyBlock.getBlockSequence().close();
        keyBlock.getBlockSequence().getBlockProvider().getBlockContainer().close();
    }

    @Override
    public Status getStatus() {
        return keyBlock.getBlockSequence().getStatus();
    }

    @Override
    public void commit() {
        keyBlock.getBlockSequence().getBlockProvider().getBlockContainer().commit();
    }
}
