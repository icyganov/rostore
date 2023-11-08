package org.rostore.entity.media;

public class ContainerMetaCompatibility {
    private long creationTime;
    private long maxSize;
    private int segmentNumber;
    private int shardNumber;
    private long maxTTL;

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public int getSegmentNumber() {
        return segmentNumber;
    }

    public void setSegmentNumber(int segmentNumber) {
        this.segmentNumber = segmentNumber;
    }

    public int getShardNumber() {
        return shardNumber;
    }

    public void setShardNumber(int shardNumber) {
        this.shardNumber = shardNumber;
    }

    public long getMaxTTL() {
        return maxTTL;
    }

    public void setMaxTTL(long maxTTL) {
        this.maxTTL = maxTTL;
    }
}
