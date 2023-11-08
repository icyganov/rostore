package org.rostore.v2.container;

import org.rostore.entity.MemoryAllocationState;
import org.rostore.entity.RoStoreException;
import org.rostore.entity.media.ContainerMeta;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.allocator.Properties;
import org.rostore.v2.media.block.container.Status;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Container implements Closeable {

    private static final Logger logger = Logger.getLogger(Container.class.getName());

    private final ContainerDescriptor descriptor;
    private final ContainerListOperations containerListOperations;
    private final ContainerShard[] shards;
    private final String name;

    private Status status = Status.OPENED;

    public MemoryAllocationState getMemoryAllocation() {
        final MemoryAllocationState memoryAllocationState = MemoryAllocationState.init();
        for(int i = 0; i< descriptor.getContainerMeta().getShardNumber(); i++) {
            final ContainerShard containerShard = getShard(i);
            memoryAllocationState.plus(containerShard.getMemoryAllocation());
        }
        return memoryAllocationState;
    }

    public String getName() {
        return name;
    }

    public ContainerDescriptor getDescriptor() {
        return descriptor;
    }

    public ContainerListOperations getContainerListOperations() {
        return containerListOperations;
    }

    public static final void checkFreeSpaceToCreateContainer(final String name, final Media media, final ContainerMeta containerMeta) {
        if (containerMeta.getMaxSize() > media.getMediaProperties().getMaxTotalSize()) {
            throw new RoStoreException("Can't create container \"" + name + "\" with max size " +
                    containerMeta.getMaxSize() +
                    "B, for media of max size " +
                    media.getMediaProperties().getMaxTotalSize()
                    + "B.");
        }
        long totalFreeSize = media.getMemoryManagement().getLockedFreeSize();
        // allocator size + key + 1 (header)
        long minShardSize = (Properties.SECONDARY_ALLOCATOR_CACHE_MAX_SIZE + org.rostore.v2.seq.Properties.MAX_FREE_BLOCK_NUMBER + 1);
        // shard sizes + 1 (header)
        long minSize = containerMeta.getShardNumber() * minShardSize + 1;
        long toleratedSize = minSize * 2;
        if (toleratedSize > totalFreeSize) {
            throw new RoStoreException("Can't create container \"" + name + "\"  with tolerated size " +
                    toleratedSize +
                    "B, as media has " +
                    totalFreeSize
                    + "B of free space.");
        }
        if (containerMeta.getMaxSize() != 0 && toleratedSize > containerMeta.getMaxSize()) {
            throw new RoStoreException("Can't create container \"" + name + "\"  with tolerated size " +
                    toleratedSize +
                    "B, and requested max size of " +
                    containerMeta.getMaxSize()
                    + "B.");
        }
    }

    /**
     * This one opens up the container if it has already been created
     * @param containerListOperations
     * @param name
     * @param descriptor
     */
    public Container(final ContainerListOperations containerListOperations, final String name, final ContainerDescriptor descriptor) {
        this.name = name;
        this.containerListOperations = containerListOperations;
        this.descriptor = descriptor;
        shards = new ContainerShard[descriptor.getContainerMeta().getShardNumber()];
    }

    public void remove() {
        for(int i=0; i<shards.length; i++) {
            ContainerShard shard = shards[i];
            if (shard == null) {
                shard = ContainerShard.open(this, i, this.descriptor.getShardDescriptors().get(i));
            }
            shard.remove();
        }
    }

    /**
     * This constructor creates a new container
     * @param containerListOperations
     * @param name
     * @param containerMeta
     */
    public Container(final ContainerListOperations containerListOperations, final String name, final ContainerMeta containerMeta) {
        checkFreeSpaceToCreateContainer(name, containerListOperations.getMedia(), containerMeta);
        this.name = name;
        this.containerListOperations = containerListOperations;
        this.descriptor = new ContainerDescriptor(containerMeta);
        shards = new ContainerShard[descriptor.getContainerMeta().getShardNumber()];
        try {
            for (int i = 0; i < descriptor.getContainerMeta().getShardNumber(); i++) {
                final ContainerShard containerShard = ContainerShard.create(this, i);
                descriptor.getShardDescriptors().add(containerShard.getDescriptor());
                shards[i] = containerShard;
            }
        } catch (final Exception e) {
            for (int i = 0; i < descriptor.getContainerMeta().getShardNumber(); i++) {
                if (shards[i] != null) {
                    shards[i].remove();
                }
            }
            throw e;
        }
    }

    public ContainerShard getShard(final int shardIndex) {
        if (shardIndex<0 || shardIndex>=getDescriptor().getContainerMeta().getShardNumber()) {
            throw new RoStoreException("There is no shard with index=" + shardIndex + ", max=" + getDescriptor().getContainerMeta().getShardNumber());
        }
        ContainerShard shard = shards[shardIndex];
        if (shard == null) {
            synchronized (this) {
                // second try in case somebody allocated it mean time
                shard = shards[shardIndex];
                if (shard == null) {
                    if (shardIndex < 0 || shardIndex >= getDescriptor().getContainerMeta().getShardNumber()) {
                        throw new RoStoreException("Invalid shard index " + shardIndex + ", num=" + getDescriptor().getContainerMeta().getShardNumber());
                    }
                    shard = ContainerShard.open(this, shardIndex, descriptor.getShardDescriptors().get(shardIndex));
                    shards[shardIndex] = shard;
                }
            }
        }
        return shard;
    }

    @Override
    public void close() {
        status = Status.CLOSED;
        for(final ContainerShard shard : shards) {
            try {
                if (shard != null) {
                    shard.close();
                }
            } catch (final Exception e) {
                logger.log(Level.SEVERE, "Error in closing the container", e);
            }
        }
    }

    @Override
    public Status getStatus() {
        return status;
    }
}
