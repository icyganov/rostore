package org.rostore.v2.container;

import org.rostore.mapper.BlockIndex;

public class ContainerShardDescriptor {

    @BlockIndex
    private long allocatorStartIndex;

    @BlockIndex
    private long keysStartIndex;

    public long getAllocatorStartIndex() {
        return allocatorStartIndex;
    }

    public long getKeysStartIndex() {
        return keysStartIndex;
    }

    public ContainerShardDescriptor(long allocatorStartIndex, long keysStartIndex) {
        this.allocatorStartIndex = allocatorStartIndex;
        this.keysStartIndex = keysStartIndex;
    }

    /**
     * Needed for load
     */
    public ContainerShardDescriptor() {}
}
