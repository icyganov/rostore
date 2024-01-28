package org.rostore.v2.container;

import org.rostore.entity.BlockAllocation;
import org.rostore.entity.Record;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.data.DataReader;
import org.rostore.v2.data.DataWriter;
import org.rostore.v2.keys.KeyBlockOperations;
import org.rostore.v2.keys.RecordLengths;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.block.container.Status;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

/**
 * Container Shards ar independent portion of the Container, that have its own {@link BlockAllocator}
 * and independent set of keys. The user of the container should implement a function that
 * determines which shard should be used. Typically, some sort of hash function that splits
 * the space of the keys can be applied to partition it into the shards.
 * <p>Container itself does not provide functionality of that kind and only
 * offers the access to the shards.</p>
 * <p>The shards are using their own {@link BlockAllocator}, which allows the independent allocation of the
 * blocks and also allows to remove the shard without knowing of its internal structure. Everything
 * stored with this allocator can be removed when the allocator is removed.</p>
 */
public class ContainerShard implements Closeable {

    private final Container container;
    private final int index;

    private final ContainerShardDescriptor descriptor;

    private BlockAllocator shardAllocator;

    /**
     * A set of currently executed allocated operations
     */
    private List<ContainerShardKeyOperations> used = new ArrayList<>();

    /**
     * A set of cached key operations to be reused
     */
    private Queue<ContainerShardKeyOperations> unused = new LinkedList<>();

    public BlockAllocator getShardAllocator() {
        return shardAllocator;
    }

    public ContainerShardDescriptor getDescriptor() {
        return descriptor;
    }

    public int getIndex() {
        return index;
    }

    public Container getContainer() {
        return container;
    }

    /**
     * Opens a shard of the container
     *
     * @param container the parent container
     * @param index the index of the shard
     * @param containerShardDescriptor the information about properties of the shard (should be retrieved from the underlying media)
     * @return the shard object
     */
    protected static ContainerShard open(final Container container, final int index, final ContainerShardDescriptor containerShardDescriptor) {
        return new ContainerShard(container, index, containerShardDescriptor);
    }

    /**
     * Creates a new shard in the storage
     *
     * <p>As the shard requires its own resources, the {@link Media#getRootBlockAllocator()} is used to create the obtain them.</p>
     *
     * @param container the parent container
     * @param index the index if the shard
     * @return the shard object
     */
    protected static ContainerShard create(final Container container, final int index) {
        return new ContainerShard(container, index);
    }

    /**
     * Loader.
     */
    private ContainerShard(final Container container, final int index, final ContainerShardDescriptor descriptor) {
        this.container = container;
        this.index = index;
        this.descriptor = descriptor;
        this.shardAllocator = container.getContainerListOperations().getMedia().loadSecondaryBlockAllocator(
                shardAllocatorName(),
                descriptor.getAllocatorStartIndex(),
                maxBlockNumber());
    }

    private String shardAllocatorName() {
        return "secondary:" + container.getName();
    }

    /**
     * Creator
     */
    private ContainerShard(final Container container, final int index) {
        this.index = index;
        this.container = container;
        this.shardAllocator = container.getContainerListOperations().getMedia().createSecondaryBlockAllocator(shardAllocatorName(),
                maxBlockNumber());
        try (final KeyBlockOperations keyBlockOperations = KeyBlockOperations.create(shardAllocator,
                RecordLengths.standardRecordLengths(container.getContainerListOperations().getMedia().getMediaProperties()))) {
            this.descriptor = new ContainerShardDescriptor(shardAllocator.getStartIndex(),
                    keyBlockOperations.getStartIndex());
        }
    }

    /**
     * Provides information about block allocation within the shard
     *
     * @return the block allocation
     */
    public BlockAllocation getBlockAllocation() {
        return shardAllocator.getBlockAllocation();
    }

    /**
     * Function to store the body of the value, using the shard's allocator
     *
     * @param data the data to be stored
     * @return the block index where the data is stored
     * @param <T> the type of the input stream
     */
    public <T extends InputStream> long putValue(final T data) {
        return DataWriter.fromInputStream(shardAllocator, data);
    }

    /**
     * Function to store the body of the value, using the shard's allocator
     * @param record the record information to retrieve the block where the data starts
     * @param outputStream the stream to put the data to
     * @param <T> the type of the output stream
     */
    public <T extends OutputStream> void getValue(final Record record, final T outputStream) {
        DataReader.toOutputStream(shardAllocator.getMedia(), record.getId(), outputStream);
    }

    /**
     * Function to remove the value from the shard's allocator and free the blocks used by it
     * @param id the block index where the data starts
     */
    public void removeValue(final long id) {
        try (final DataReader dataReader = DataReader.open(shardAllocator, id)) {
            dataReader.free();
        }
    }

    private long maxBlockNumber() {
        return container.getDescriptor().getContainerMeta().getMaxSize() / container.getContainerListOperations().getMedia().getMediaProperties().getBlockSize();
    }

    public String toString() {
        return "Shard: index=" + index;
    }

    /**
     * Removes all the data associated with the shard
     */
    public void remove() {
        if (!used.isEmpty()) {
            throw new RoStoreException("Can't remove the shard as it is in use");
        }
        for(final ContainerShardKeyOperations ops : unused) {
            ops.close();
        }
        shardAllocator.remove();
    }

    @Override
    public void close() {
        if (!used.isEmpty()) {
            throw new RoStoreException("Can't close the shard as it is in use");
        }
        try {
            for (final ContainerShardKeyOperations ops : unused) {
                ops.close();
            }
        } finally {
            shardAllocator.close();
        }
    }

    @Override
    public Status getStatus() {
        return shardAllocator.getStatus();
    }

    private ContainerShardKeyOperations poll() {
        synchronized (this) {
            if (!unused.isEmpty()) {
                final ContainerShardKeyOperations ops = unused.poll();
                used.add(ops);
                return ops;
            }
        }

        final ContainerShardKeyOperations ops = new ContainerShardKeyOperations(this);
        synchronized (this) {
            used.add(ops);
            return ops;
        }

    }

    private void done(final ContainerShardKeyOperations ops) {
        try {
            ops.commit();
        } finally {
            boolean close = true;
            synchronized (this) {
                used.remove(ops);
                if (unused.size() < container.getContainerListOperations().getContainerListHeader().getContainerListProperties().getMaxKeyOperationsPerShard()) {
                    unused.offer(ops);
                    close = false;
                }
            }
            if (close) {
                ops.close();
            }
        }
    }

    /**
     * Executes an operation on the shard's keys catalog.
     * <p>The function will provide a cached {@link ContainerShardKeyOperations} if one exists, or
     * create one if cache is depleted.</p>
     * <p>After the operation is over the {@link org.rostore.v2.catalog.CachedCatalogBlockOperations} will be
     * returned to the cache, so it can be reused later.</p>
     * <p>The operation does not do any blocking, for example for write-operations, it should be
     * enforced by the caller.</p>
     *
     * @param keyFunction that will receive a {@link ContainerShardKeyOperations} instance to execute the operation
     * @return the result of the function execution
     * @param <T> the result type, e.g. {@link Record}
     */
    public <T> T keyFunction(final Function<ContainerShardKeyOperations, T> keyFunction) {
        final ContainerShardKeyOperations ops = poll();
        try {
            return keyFunction.apply(ops);
        } finally {
            done(ops);
        }
    }

}
