package org.rostore.v2.seq;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.catalog.CatalogBlockIndices;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the list of blocks which is used in the sequences
 *
 * <p>Every block starts with nextBlock reference.</p>
 */
public class BlockIndexSequence implements AutoCloseable {

    private final List<Long> sequenceBlockIndices = new ArrayList<>();

    private int refs = 0;

    private long lastUsageTimestampMillis = 0;

    private int firstFreeIndex = -1;

    public void removeAtEnd(int howMany) {
        while(howMany != 0) {
            sequenceBlockIndices.remove(sequenceBlockIndices.size()-1);
            howMany--;
        }
    }

    public CatalogBlockIndices createCatalogBlockIndices() {
        CatalogBlockIndices catalogBlockIndices = new CatalogBlockIndices();
        for(long blockIndex : sequenceBlockIndices) {
            catalogBlockIndices.add(blockIndex, blockIndex);
        }
        return catalogBlockIndices;
    }

    public void markSequenceUsed() {
        lastUsageTimestampMillis = System.currentTimeMillis();
        refs++;
    }

    public void close() {
        if (refs == 0) {
            throw new RoStoreException("Number of block sequence ref is negative");
        }
        refs--;
    }

    public boolean isSequenceInUse() {
        return refs > 0;
    }

    public long getBlockIndex(int seqIndex) {
        return sequenceBlockIndices.get(seqIndex);
    }

    public int length() {
        return sequenceBlockIndices.size();
    }

    public int getFreeBlockNumber() {
        return length() - getFirstFreeIndex();
    }

    public void add(int seqIndex, long blockIndex) {
        sequenceBlockIndices.add(seqIndex, blockIndex);
    }

    public void remove(int seqIndex) {
        sequenceBlockIndices.remove(seqIndex);
    }

    public long getLastUsageTimestampMillis() {
        return lastUsageTimestampMillis;
    }

    public int getFirstFreeIndex() {
        return firstFreeIndex;
    }

    public void setFirstFreeIndex(final int firstFreeIndex) {
        if (firstFreeIndex > sequenceBlockIndices.size()) {
            throw new RoStoreException("Can't set the first index=\"" + firstFreeIndex + "\" when the sequence size=\"" + sequenceBlockIndices.size() + "\"");
        }
        this.firstFreeIndex = firstFreeIndex;
    }

}
