package org.rostore.v2.container;

import org.rostore.entity.MemoryAllocation;
import org.rostore.entity.Record;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.data.DataReader;
import org.rostore.v2.data.DataWriter;
import org.rostore.v2.keys.KeyBlockOperations;
import org.rostore.v2.keys.RecordLengths;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.block.container.Status;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

public class ContainerShard implements Closeable {

    private final Container container;
    private final int index;

    private final ContainerShardDescriptor descriptor;

    private BlockAllocator shardAllocator;

    private List<ContainerShardKeyOperations> used = new ArrayList<>();
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

    public static ContainerShard open(final Container container, final int index, final ContainerShardDescriptor containerShardDescriptor) {
        return new ContainerShard(container, index, containerShardDescriptor);
    }

    public static ContainerShard create(final Container container, final int index) {
        return new ContainerShard(container, index);
    }

    /**
     * Loader
     * @param container
     * @param descriptor
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
     * @param index
     * @param container
     * @param index
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

    public MemoryAllocation getMemoryAllocation() {
        return shardAllocator.getMemoryAllocation();
    }

    public <T extends InputStream> long putValue(final T data) {
        return DataWriter.fromInputStream(shardAllocator, data);
    }

    public <T extends OutputStream> void getValue(final Record record, final T outputStream) {
        DataReader.toOutputStream(shardAllocator.getMedia(), record.getId(), outputStream);
    }

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

    public <T> T keyFunction(final Function<ContainerShardKeyOperations, T> opsConsumer) {
        ContainerShardKeyOperations ops = poll();
        try {
            return opsConsumer.apply(ops);
        } finally {
            done(ops);
        }
    }

}
