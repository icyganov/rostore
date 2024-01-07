package org.rostore.v2.container;

import org.rostore.mapper.BlockIndex;

/**
 * This information meant to be serialized and stored to the
 * {@link org.rostore.v2.media.Media} so that the shard can be recreated.
 */
public class ContainerShardDescriptor {

    @BlockIndex
    private long allocatorStartIndex;

    @BlockIndex
    private long keysStartIndex;

    /**
     * The first block of the allocator's sequence
     * @return the block index
     */
    public long getAllocatorStartIndex() {
        return allocatorStartIndex;
    }

    /**
     * The first block index of the of key catalog's sequence of the shard
     * @return the first index
     */
    public long getKeysStartIndex() {
        return keysStartIndex;
    }

    /**
     * Creates the descriptor
     * @param allocatorStartIndex the first block in the shard's allocator sequence
     * @param keysStartIndex the first block in the shard's key catalog sequence
     */
    public ContainerShardDescriptor(long allocatorStartIndex, long keysStartIndex) {
        this.allocatorStartIndex = allocatorStartIndex;
        this.keysStartIndex = keysStartIndex;
    }

    /**
     * Needed for load
     */
    public ContainerShardDescriptor() {}
}
