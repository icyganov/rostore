package org.rostore.entity.media;

/**
 * The class contains the properties that steers the list of containers.
 * <p>This information is provided at the storage creation and can't be changed later.</p>
 */
public class ContainerListProperties {

    /**
     * If the container stays idle longer than this, it can be closed...
     */
    private static final long AUTOCLOSE_CONTAINERS_AFTER_MILLIS_DEFAULT = 60*1000;

    public static final long CLEANUP_INTERVAL_MILLIS_DEFAULT = 5000;
    public static final int MAX_CLEANS_PER_CYCLE_DEFAULT = 5;

    public static final int LIST_MAX_CONTAINERS_DEFAULT = 1024;
    public static final long LIST_MAX_SIZE_DEFAULT = 1024 * 1024 * 5;

    /**
     * Number of opened KeyOperations object for every opened shard
     */
    public static final int MAX_KEY_OPERATIONS_PER_SHARD_DEFAULT = 10;

    private long autoCloseContainersAfterMillis = AUTOCLOSE_CONTAINERS_AFTER_MILLIS_DEFAULT;

    private long cleanupIntervalMillis = CLEANUP_INTERVAL_MILLIS_DEFAULT;

    private int maxCleanupsPerCycle = MAX_CLEANS_PER_CYCLE_DEFAULT;

    private int maxContainersPerList = LIST_MAX_CONTAINERS_DEFAULT;

    private long maxContainersListSize = LIST_MAX_SIZE_DEFAULT;

    private int maxKeyOperationsPerShard = MAX_KEY_OPERATIONS_PER_SHARD_DEFAULT;

    public long getAutoCloseContainersAfterMillis() {
        return autoCloseContainersAfterMillis;
    }

    public void setAutoCloseContainersAfterMillis(long autoCloseContainersAfterMillis) {
        this.autoCloseContainersAfterMillis = autoCloseContainersAfterMillis;
    }

    public long getCleanupIntervalMillis() {
        return cleanupIntervalMillis;
    }

    public void setCleanupIntervalMillis(long cleanupIntervalMillis) {
        this.cleanupIntervalMillis = cleanupIntervalMillis;
    }

    public int getMaxCleanupsPerCycle() {
        return maxCleanupsPerCycle;
    }

    public void setMaxCleanupsPerCycle(int maxCleanupsPerCycle) {
        this.maxCleanupsPerCycle = maxCleanupsPerCycle;
    }

    /**
     * The maximum number of containers in the storage
     *
     * @return the number of containers.
     */
    public int getMaxContainersPerList() {
        return maxContainersPerList;
    }

    /**
     * Sets the maximum number of containers in the storage.
     *
     * @param maxContainersPerList The maximum number of containers in the storage
     */
    public void setMaxContainersPerList(int maxContainersPerList) {
        this.maxContainersPerList = maxContainersPerList;
    }

    /**
     * The maximum size in bytes of all container names in the list
     * @return the size of all container names in the list in bytes
     */
    public long getMaxContainersListSize() {
        return maxContainersListSize;
    }

    /**
     * Sets the maximum size in bytes of all container names in the list
     * @param maxContainersListSize maximum size in bytes of all container names in the list
     */
    public void setMaxContainersListSize(long maxContainersListSize) {
        this.maxContainersListSize = maxContainersListSize;
    }

    /**
     * The maximum number of key operations that the shard would support in parallel.
     * <p>Only key-read operations would be executed in parallel. Write operation is all the time blocking.</p>
     * <p>The jey operation context of these amount is kept cached in memory and get reused.</p>
     * <p>If more operation is executed, they get queued and will be executed in order of their arrival.</p>
     * @return the maximum operations executed per shard in parallel
     */
    public int getMaxKeyOperationsPerShard() {
        return maxKeyOperationsPerShard;
    }

    /**
     * Sets the number of parallel operations per shard.
     *
     * @param maxKeyOperationsPerShard the number of parallel operations per shard.
     */
    public void setMaxKeyOperationsPerShard(int maxKeyOperationsPerShard) {
        this.maxKeyOperationsPerShard = maxKeyOperationsPerShard;
    }
}
