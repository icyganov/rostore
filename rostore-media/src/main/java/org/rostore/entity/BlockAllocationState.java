package org.rostore.entity;

/**
 * Materialized object that implements {@link BlockAllocation}
 */
public class BlockAllocationState implements BlockAllocation {

    private long payloadSize, totalLockedSize, lockedFreeSize;

    public static BlockAllocationState init() {
        return new BlockAllocationState();
    }


    /**
     * Initializes the state object
     *
     * @param totalLockedSize the total size of the locked space
     * @param lockedFreeSize number of bytes free in the total locked size
     * @param payloadSize the used bytes number of total locked
     * @return the persistent object
     */
    public static BlockAllocationState init(final long totalLockedSize, final long lockedFreeSize, final long payloadSize) {
        BlockAllocationState memoryAllocationState = new BlockAllocationState();
        memoryAllocationState.totalLockedSize = totalLockedSize;
        memoryAllocationState.lockedFreeSize = lockedFreeSize;
        memoryAllocationState.payloadSize = payloadSize;
        return memoryAllocationState;
    }

    /**
     * Creates a static copy of the provided {@link BlockAllocation}
     *
     * @param blockAllocation the object to get data from
     * @return the persistent block allocation
     */
    public static BlockAllocationState store(final BlockAllocation blockAllocation) {
        return new BlockAllocationState(blockAllocation);
    }

    /**
     * Subtracts the sizes from the provided object to this object
     *
     * @param blockAllocation the provided object
     */
    public synchronized void minus(final BlockAllocation blockAllocation) {
        payloadSize -= blockAllocation.getPayloadSize();
        totalLockedSize -= blockAllocation.getTotalLockedSize();
        lockedFreeSize -= blockAllocation.getLockedFreeSize();
    }

    /**
     * Adds the sizes from the provided object to this object
     *
     * @param blockAllocation the provided object
     */
    public synchronized void plus(final BlockAllocation blockAllocation) {
        payloadSize += blockAllocation.getPayloadSize();
        totalLockedSize += blockAllocation.getTotalLockedSize();
        lockedFreeSize += blockAllocation.getLockedFreeSize();
    }

    private BlockAllocationState() {
        totalLockedSize = 0;
        payloadSize = 0;
        lockedFreeSize = 0;
    }

    private BlockAllocationState(final long payloadSize) {
        totalLockedSize = payloadSize;
        this.payloadSize = payloadSize;
        lockedFreeSize = 0;
    }

    private BlockAllocationState(final BlockAllocation blockAllocation) {
        totalLockedSize = blockAllocation.getTotalLockedSize();
        payloadSize = blockAllocation.getPayloadSize();
        lockedFreeSize = blockAllocation.getLockedFreeSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getPayloadSize() {
        return payloadSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalLockedSize() {
        return totalLockedSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLockedFreeSize() {
        return lockedFreeSize;
    }

    @Override
    public String toString() {
        return "MemoryAllocationState{" +
                "payload=" + payloadSize +
                ", totalLocked=" + totalLockedSize +
                ", lockedFree=" + lockedFreeSize +
                '}';
    }
}
