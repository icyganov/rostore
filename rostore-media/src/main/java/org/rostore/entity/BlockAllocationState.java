package org.rostore.entity;

/**
 * Materialized object that implements {@link BlockAllocation}
 */
public class BlockAllocationState implements BlockAllocation {

    private long payloadSize, totalLockedSize, lockedFreeSize;

    public static BlockAllocationState init() {
        return new BlockAllocationState();
    }


    public static BlockAllocationState init(long totalLockedSize, long lockedFreeSize, long payloadSize) {
        BlockAllocationState memoryAllocationState = new BlockAllocationState();
        memoryAllocationState.totalLockedSize = totalLockedSize;
        memoryAllocationState.lockedFreeSize = lockedFreeSize;
        memoryAllocationState.payloadSize = payloadSize;
        return memoryAllocationState;
    }

    public static BlockAllocationState store(final BlockAllocation blockAllocation) {
        return new BlockAllocationState(blockAllocation);
    }

    public synchronized void minus(final BlockAllocation blockAllocation) {
        payloadSize -= blockAllocation.getPayloadSize();
        totalLockedSize -= blockAllocation.getTotalLockedSize();
        lockedFreeSize -= blockAllocation.getLockedFreeSize();
    }

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

    @Override
    public long getPayloadSize() {
        return payloadSize;
    }

    @Override
    public long getTotalLockedSize() {
        return totalLockedSize;
    }

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
