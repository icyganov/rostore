package org.rostore.v2.media;

public class MemoryConsumption {

    private final int blocksAllocated;
    private final int blockSequencesAllocated;
    private final int blockContainerAllocated;

    public MemoryConsumption(int blocksAllocated, int blockSequencesAllocated, int blockContainerAllocated) {
        this.blocksAllocated = blocksAllocated;
        this.blockSequencesAllocated = blockSequencesAllocated;
        this.blockContainerAllocated = blockContainerAllocated;
    }

    public int getBlocksAllocated() {
        return blocksAllocated;
    }

    public int getBlockSequencesAllocated() {
        return blockSequencesAllocated;
    }

    public int getBlockContainerAllocated() {
        return blockContainerAllocated;
    }

}
