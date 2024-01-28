package org.rostore.entity.media;

/**
 * This one is needed to create MediaProperties. As the MediaProperties are immutable
 * once the media is created, it can't expose any changeable fields, yet at the
 * creation phase one needs to set them. So, the builder is used to initially populate the
 * fields that are frozen once the builder is transformed to MediaProperties
 * after the Media creation.
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

    /**
     * Sets the maximum total size of the storage
     *
     * @param maxTotalSize the maximum total size in bytes
     * @return the builder object
     */
    public MediaPropertiesBuilder maxTotalSize(final long maxTotalSize) {
        this.maxTotalSize = maxTotalSize;
        return this;
    }

    /**
     * Sets the block size that will be used by the storage
     *
     * <p>It should be aligned with the physical memory page in the hosting
     * system to achieve the best alignment and memory usage.</p>
     *
     * @param blockSize the block size in bytes
     * @return the builder object
     */
    public MediaPropertiesBuilder blockSize(final int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    /**
     * Sets the time interval in milliseconds after each the unused blocks will
     * be removed from the memory.
     * <p>The caching of unused block is only used fo the blocks of CATALOG or KEY types.</p>
     *
     * @param closeUnusedBlocksAfterMillis time interval to remove the unused blocks from the memory
     * @return the builder object
     */
    public MediaPropertiesBuilder closeUnusedBlocksAfterMillis(final long closeUnusedBlocksAfterMillis) {
        this.closeUnusedBlocksAfterMillis = closeUnusedBlocksAfterMillis;
        return this;
    }

    /**
     * Sets the time interval in milliseconds after each the unused block sequences will
     * be removed from the memory.
     *
     * <p>Sequences combine several blocks in one continues data block, which is used
     * in the ro-store controlling structures such as catalogs or key blocks.</p>
     *
     * @param closeUnusedSequencesAfterMillis the time interval in milliseconds
     * @return the builder object
     */
    public MediaPropertiesBuilder closeUnusedSequencesAfterMillis(final long closeUnusedSequencesAfterMillis) {
        this.closeUnusedSequencesAfterMillis = closeUnusedSequencesAfterMillis;
        return this;
    }

    /**
     * Provides the total maximum size of the storage
     *
     * @return the size in bytes
     */
    public long getMaxTotalSize() {
        return maxTotalSize;
    }

    /**
     * Sets the maximum total size of the storage
     *
     * @param maxTotalSize the maximum total size in bytes
     */
    public void setMaxTotalSize(long maxTotalSize) {
        this.maxTotalSize = maxTotalSize;
    }

    /**
     * Provides the block size
     *
     * @return the block size in bytes
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Sets the block size that will be used by the storage
     *
     * <p>It should be aligned with the physical memory page in the hosting
     * system to achieve the best alignment and memory usage.</p>
     *
     * @param blockSize the block size in bytes
     */
    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    /**
     * Provides the time interval in milliseconds after each the unused blocks will
     * be removed from the memory.
     *
     * <p>The caching of unused block is only used fo the blocks of CATALOG or KEY types.</p>
     * @return the time interval in milliseconds
     */
    public long getCloseUnusedBlocksAfterMillis() {
        return closeUnusedBlocksAfterMillis;
    }

    /**
     * Sets the time interval in milliseconds after each the unused blocks will
     * be removed from the memory.
     * <p>The caching of unused block is only used fo the blocks of CATALOG or KEY types.</p>
     *
     * @param closeUnusedBlocksAfterMillis time interval to remove the unused blocks from the memory
     */
    public void setCloseUnusedBlocksAfterMillis(long closeUnusedBlocksAfterMillis) {
        this.closeUnusedBlocksAfterMillis = closeUnusedBlocksAfterMillis;
    }

    /**
     * Gets the time interval in milliseconds after each the unused block sequences will
     * be removed from the memory.
     *
     * <p>Sequences combine several blocks in one continues data block, which is used
     * in the ro-store controlling structures such as catalogs or key blocks.</p>
     *
     * @return the time interval in milliseconds
     */
    public long getCloseUnusedSequencesAfterMillis() {
        return closeUnusedSequencesAfterMillis;
    }

    /**
     * Sets the time interval in milliseconds after each the unused block sequences will
     * be removed from the memory.
     *
     * <p>Sequences combine several blocks in one continues data block, which is used
     * in the ro-store controlling structures such as catalogs or key blocks.</p>
     *
     * @param closeUnusedSequencesAfterMillis the time interval in milliseconds
     */
    public void setCloseUnusedSequencesAfterMillis(long closeUnusedSequencesAfterMillis) {
        this.closeUnusedSequencesAfterMillis = closeUnusedSequencesAfterMillis;
    }
}
