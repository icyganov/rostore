package org.rostore.v2.media;

/**
 * This object is stored in the first block of ro-store {@link Media}.
 * <p>It is populated from the {@link MediaProperties}.</p>
 */
public class MediaHeader {

    private int blockSize;
    private long maxTotalSize;
    private byte magic;
    private long closeUnusedBlocksAfterMillis;
    private long closeUnusedSequencesAfterMillis;

    /**
     * Provides a magic byte that is used for verification
     *
     * @return the magic byte
     */
    public byte getMagic() {
        return magic;
    }

    /**
     * Sets the magic byte to the header, which will be used for verification
     *
     * @param magic the magic byte
     */
    public void setMagic(byte magic) {
        this.magic = magic;
    }

    /**
     * Persists {@link MediaProperties#getMaxTotalSize()}
     */
    public long getMaxTotalSize() {
        return maxTotalSize;
    }

    /**
     * Persists {@link MediaProperties#getMaxTotalSize()}
     */
    public void setMaxTotalSize(long maxTotalSize) {
        this.maxTotalSize = maxTotalSize;
    }

    /**
     * Persists {@link MediaProperties#getBlockSize()}
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Persists {@link MediaProperties#getBlockSize()}
     */
    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }
    /**
     * Persists {@link MediaProperties#getCloseUnusedBlocksAfterMillis()} ()}
     */
    public long getCloseUnusedBlocksAfterMillis() {
        return closeUnusedBlocksAfterMillis;
    }

    /**
     * Persists {@link MediaProperties#getCloseUnusedBlocksAfterMillis()} ()}
     */
    public void setCloseUnusedBlocksAfterMillis(long closeUnusedBlocksAfterMillis) {
        this.closeUnusedBlocksAfterMillis = closeUnusedBlocksAfterMillis;
    }

    /**
     * Persists {@link MediaProperties#getCloseUnusedSequencesAfterMillis()}
     */
    public long getCloseUnusedSequencesAfterMillis() {
        return closeUnusedSequencesAfterMillis;
    }

    /**
     * Persists {@link MediaProperties#getCloseUnusedSequencesAfterMillis()}
     */
    public void setCloseUnusedSequencesAfterMillis(long closeUnusedSequencesAfterMillis) {
        this.closeUnusedSequencesAfterMillis = closeUnusedSequencesAfterMillis;
    }
}
