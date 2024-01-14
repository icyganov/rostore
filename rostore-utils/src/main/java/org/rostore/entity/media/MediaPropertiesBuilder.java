package org.rostore.entity.media;

/**
 * This one needed to create media properties.
 */
public class MediaPropertiesBuilder {
    private static final long CLOSE_UNUSED_BLOCKS_AFTER_MILLIS = 5*1000;
    private static final long CLOSE_UNUSED_SEQUENCES_AFTER_MILLIS = 5*1000;

    private static final long TOTAL_MAX_SIZE = 100L*1024L*1024L;

    private static final int BLOCK_SIZE = 4096;

    private long maxTotalSize = TOTAL_MAX_SIZE;
    private int blockSize = BLOCK_SIZE;

    private long closeUnusedBlocksAfterMillis =  CLOSE_UNUSED_BLOCKS_AFTER_MILLIS;
    private long closeUnusedSequencesAfterMillis =  CLOSE_UNUSED_SEQUENCES_AFTER_MILLIS;

    public MediaPropertiesBuilder maxTotalSize(final long maxTotalSize) {
        this.maxTotalSize = maxTotalSize;
        return this;
    }

    public MediaPropertiesBuilder blockSize(final int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public MediaPropertiesBuilder closeUnusedBlocksAfterMillis(final long closeUnusedBlocksAfterMillis) {
        this.closeUnusedBlocksAfterMillis = closeUnusedBlocksAfterMillis;
        return this;
    }

    public MediaPropertiesBuilder closeUnusedSequencesAfterMillis(final long closeUnusedSequencesAfterMillis) {
        this.closeUnusedSequencesAfterMillis = closeUnusedSequencesAfterMillis;
        return this;
    }

    public long getMaxTotalSize() {
        return maxTotalSize;
    }

    public void setMaxTotalSize(long maxTotalSize) {
        this.maxTotalSize = maxTotalSize;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public long getCloseUnusedBlocksAfterMillis() {
        return closeUnusedBlocksAfterMillis;
    }

    public void setCloseUnusedBlocksAfterMillis(long closeUnusedBlocksAfterMillis) {
        this.closeUnusedBlocksAfterMillis = closeUnusedBlocksAfterMillis;
    }

    public long getCloseUnusedSequencesAfterMillis() {
        return closeUnusedSequencesAfterMillis;
    }

    public void setCloseUnusedSequencesAfterMillis(long closeUnusedSequencesAfterMillis) {
        this.closeUnusedSequencesAfterMillis = closeUnusedSequencesAfterMillis;
    }
}
