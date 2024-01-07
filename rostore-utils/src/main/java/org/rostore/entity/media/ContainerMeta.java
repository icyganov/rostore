package org.rostore.entity.media;

/**
 * Specifies the major container properties, such as the maximum size and maximum TTL of the keys
 * the container will allow to store, as well as the number of container's shard.
 *
 * <p>These values can only be set at the time of container creation.</p>
 */
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

    /**
     * The maximum size of the container.
     * <p>The criteria is loosely checked, but will eventually be enforced.</p>
     *
     * @return the maximum size in bytes
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * Sets the maximum size of the container.
     */
    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * The number of shards the container will have.
     *
     * @return the number of shards
     */
    public int getShardNumber() {
        return shardNumber;
    }

    /**
     * Sets the number of shards the container will have.
     *
     */
    public void setShardNumber(int shardNumber) {
        this.shardNumber = shardNumber;
    }

    /**
     * The maximum TTL in seconds that the keys can have in the container.
     * <p>If the client would try to create a key with a higher TTL, it will be capped.</p>
     *
     * @return maximum TTL of the keys in the container in seconds
     */
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
