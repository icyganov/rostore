package org.rostore.entity;

public class MemoryAllocationState implements MemoryAllocation {

    private long payloadSize, totalLockedSize, lockedFreeSize;

    public static MemoryAllocationState init() {
        return new MemoryAllocationState();
    }


    public static MemoryAllocationState init(long totalLockedSize, long lockedFreeSize, long payloadSize) {
        MemoryAllocationState memoryAllocationState = new MemoryAllocationState();
        memoryAllocationState.totalLockedSize = totalLockedSize;
        memoryAllocationState.lockedFreeSize = lockedFreeSize;
        memoryAllocationState.payloadSize = payloadSize;
        return memoryAllocationState;
    }

    public static MemoryAllocationState store(final MemoryAllocation memoryAllocation) {
        return new MemoryAllocationState(memoryAllocation);
    }

    public synchronized void minus(final MemoryAllocation memoryAllocation) {
        payloadSize -= memoryAllocation.getPayloadSize();
        totalLockedSize -= memoryAllocation.getTotalLockedSize();
        lockedFreeSize -= memoryAllocation.getLockedFreeSize();
    }

    public synchronized void plus(final MemoryAllocation memoryAllocation) {
        payloadSize += memoryAllocation.getPayloadSize();
        totalLockedSize += memoryAllocation.getTotalLockedSize();
        lockedFreeSize += memoryAllocation.getLockedFreeSize();
    }

    private MemoryAllocationState() {
        totalLockedSize = 0;
        payloadSize = 0;
        lockedFreeSize = 0;
    }

    private MemoryAllocationState(final long payloadSize) {
        totalLockedSize = payloadSize;
        this.payloadSize = payloadSize;
        lockedFreeSize = 0;
    }

    private MemoryAllocationState(final MemoryAllocation memoryAllocation) {
        totalLockedSize = memoryAllocation.getTotalLockedSize();
        payloadSize = memoryAllocation.getPayloadSize();
        lockedFreeSize = memoryAllocation.getLockedFreeSize();
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
