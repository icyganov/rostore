package org.rostore.entity.media;

public class ContainerMeta {
    private long creationTime;
    private long maxSize;
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

    public int getShardNumber() {
        return shardNumber;
    }

    public void setShardNumber(int shardNumber) {
        this.shardNumber = shardNumber;
    }

    public long getMaxTTL() {
        return maxTTL;
    }

    /**
     * Sets maximum TTL of the container key
     * @param maxTTL in seconds
     */
    public void setMaxTTL(long maxTTL) {
        this.maxTTL = maxTTL;
    }
}
