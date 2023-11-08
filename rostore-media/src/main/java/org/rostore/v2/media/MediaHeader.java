package org.rostore.v2.media;

public class MediaHeader {

    private int blockSize;
    private long maxTotalSize;
    private byte magic;
    private long closeUnusedBlocksAfterMillis;
    private long closeUnusedSequencesAfterMillis;

    public byte getMagic() {
        return magic;
    }

    public void setMagic(byte magic) {
        this.magic = magic;
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
