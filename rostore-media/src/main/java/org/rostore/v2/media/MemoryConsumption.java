package org.rostore.v2.media;

import org.rostore.v2.seq.BlockIndexSequence;

/**
 * Provides information about the objects the ro-store {@link Media}
 * instance is currently keeping in memory.
 */
public class MemoryConsumption {

    private final int blocksAllocated;
    private final int blockSequencesAllocated;
    private final int blockContainerAllocated;

    protected MemoryConsumption(int blocksAllocated, int blockSequencesAllocated, int blockContainerAllocated) {
        this.blocksAllocated = blocksAllocated;
        this.blockSequencesAllocated = blockSequencesAllocated;
        this.blockContainerAllocated = blockContainerAllocated;
    }

    /**
     * Provides a number of blocks which are currently mapped from the physical
     * storage to the memory.
     *
     * @return the number of blocks allocated
     */
    public int getBlocksAllocated() {
        return blocksAllocated;
    }

    /**
     * Provides a number of currently active {@link BlockIndexSequence} that are kept in memory.
     *
     * @return the number of block sequences
     */
    public int getBlockSequencesAllocated() {
        return blockSequencesAllocated;
    }

    /**
     * Provides a number of {@link org.rostore.v2.media.block.container.BlockContainer} instances that are currently
     * open and are kept in memory.
     *
     * @return the number of block containers
     */
    public int getBlockContainerAllocated() {
        return blockContainerAllocated;
    }

}
