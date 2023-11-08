package org.rostore.entity.media;

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

    public int getMaxContainersPerList() {
        return maxContainersPerList;
    }

    public void setMaxContainersPerList(int maxContainersPerList) {
        this.maxContainersPerList = maxContainersPerList;
    }

    public long getMaxContainersListSize() {
        return maxContainersListSize;
    }

    public void setMaxContainersListSize(long maxContainersListSize) {
        this.maxContainersListSize = maxContainersListSize;
    }

    public int getMaxKeyOperationsPerShard() {
        return maxKeyOperationsPerShard;
    }

    public void setMaxKeyOperationsPerShard(int maxKeyOperationsPerShard) {
        this.maxKeyOperationsPerShard = maxKeyOperationsPerShard;
    }
}
