package org.rostore.v2.media.block.allocator;

import org.rostore.entity.MemoryAllocation;
import org.rostore.entity.MemoryAllocationState;
import org.rostore.entity.QuotaExceededException;
import org.rostore.v2.catalog.CachedCatalogBlockOperations;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.catalog.CatalogBlockOperations;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.BlockProviderImpl;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.container.Status;
import org.rostore.v2.seq.Properties;

public class RootBlockAllocator {

    private static void checkFree(final BlockAllocatorInternal blockAllocator, long requested) {
        long tolerance = org.rostore.v2.media.block.allocator.Properties.ROOT_ALLOCATOR_MIN_BUFFER;
        if (blockAllocator.getFreeBlocks() - tolerance < requested) {
            throw new QuotaExceededException("Can't allocate " + requested + " blocks. No enough free blocks (" + blockAllocator.getFreeBlocks() + "-" + tolerance + ").");
        }
    }

    public static BlockAllocator create(final Media media) {
        BlockProviderImpl blockProvider = BlockProviderImpl.internal(media);
        CatalogBlockIndices catalogBlockIndices = new CatalogBlockIndices();
        catalogBlockIndices.add(1, Properties.AVG_FREE_BLOCK_NUMBER);
        final CatalogBlockOperations rootFreeBlockOperations = CatalogBlockOperations.create(blockProvider, catalogBlockIndices);
        rootFreeBlockOperations.add(Properties.AVG_FREE_BLOCK_NUMBER+1, media.getMediaProperties().getTotalBlockNumber() - 1, false);
        final BlockAllocator rootBlockAllocator = createRootBlockAllocator(rootFreeBlockOperations);
        blockProvider.exchangeBlockAllocator(rootBlockAllocator);
        return rootBlockAllocator;
    }

    public static BlockAllocator load(final Media media) {
        BlockProviderImpl blockProvider = BlockProviderImpl.internal(media);
        final BlockAllocator rootBlockAllocator = createRootBlockAllocator(CatalogBlockOperations.load(blockProvider, 1));
        blockProvider.exchangeBlockAllocator(rootBlockAllocator);
        return rootBlockAllocator;
    }

    private static BlockAllocator createRootBlockAllocator(final CatalogBlockOperations _rootFreeBlockOperations) {
        final BlockAllocatorListeners blockAllocatorListeners = _rootFreeBlockOperations.getBlockProvider().getMedia().getBlockAllocatorListeners();
        return BlockAllocator.wrap(new BlockAllocatorInternal() {

            private CachedCatalogBlockOperations rootFreeBlockOperations = new CachedCatalogBlockOperations(_rootFreeBlockOperations,
                    org.rostore.v2.media.block.allocator.Properties.ROOT_ALLOCATOR_CACHE_MIN_SIZE,
                    org.rostore.v2.media.block.allocator.Properties.ROOT_ALLOCATOR_CACHE_MAX_SIZE);

            @Override
            public MemoryAllocation getMemoryAllocation() {
                final long freeSize = getFreeBlocks() * getMedia().getMediaProperties().getBlockSize();
                final long totalSize = getMedia().getMediaProperties().getMaxTotalSize();
                return MemoryAllocationState.init(totalSize,
                        freeSize, totalSize - freeSize);
            }

            @Override
            public synchronized long getFreeBlocks() {
                return rootFreeBlockOperations.getSequenceIndexFreeBlockNumber() + rootFreeBlockOperations.getAddedNumber() + rootFreeBlockOperations.getCachedBlockNumber();
            }

            @Override
            public synchronized CatalogBlockIndices allocate(final BlockType blockType, final int blockNumber, boolean rebalance) {
                checkFree(this, blockNumber);
                CatalogBlockIndices ret = null;
                try {
                    //System.out.println("ALLOC BEFORE: (" + this + ")");
                    //rootFreeBlockOperations.dump();
                    ret = rootFreeBlockOperations.extractIndex(blockNumber, rebalance);
                    //System.out.println("ALLOC:" + ret);
                    //System.out.println("ALLOC AFTER:");
                    //rootFreeBlockOperations.dump();
                    return ret;
                } finally {
                    //System.out.println("ALLOC FINALLY");
                    rootFreeBlockOperations.commit();
                    if (ret != null && blockAllocatorListeners != null && blockAllocatorListeners.isEnabled()) {
                        blockAllocatorListeners.notifyAllocated(getName(), blockType, ret, rebalance);
                    }
                }
            }
            @Override
            public synchronized long allocate(final BlockType blockType, boolean rebalance) {
                return allocate(blockType, 1, rebalance).iterator().get();
            }
            @Override
            public synchronized void free(final long blockIndex, boolean rebalance) {
                CatalogBlockIndices catalogBlockIndices = new CatalogBlockIndices();
                catalogBlockIndices.add(blockIndex, blockIndex);
                free(catalogBlockIndices, rebalance);
            }
            @Override
            public synchronized void free(final CatalogBlockIndices indices, boolean rebalance) {
                try {
                    if (blockAllocatorListeners != null && blockAllocatorListeners.isEnabled()) {
                        blockAllocatorListeners.notifyFreed(getName(), indices, rebalance);
                    }
                    //System.out.println("FREE:" + indices);
                    //System.out.println("FREE BEFORE: ");
                    //rootFreeBlockOperations.dump();
                    rootFreeBlockOperations.add(indices, rebalance);
                    //System.out.println("FREE AFTER:");
                    //rootFreeBlockOperations.dump();
                } finally {
                    //System.out.println("FREE FINALLY");
                    rootFreeBlockOperations.commit();
                }
            }

            @Override
            public void dump() {
                rootFreeBlockOperations.dump();
            }

            @Override
            public synchronized void close() {
                rootFreeBlockOperations.close();
            }

            @Override
            public Status getStatus() {
                return rootFreeBlockOperations.getStatus();
            }

            @Override
            public synchronized void remove() {
                rootFreeBlockOperations.remove();
            }

            @Override
            public synchronized long getStartIndex() {
                return rootFreeBlockOperations.getStartIndex();
            }

            @Override
            public Media getMedia() {
                return rootFreeBlockOperations.getMedia();
            }

            @Override
            public String getName() {
                return org.rostore.v2.media.block.allocator.Properties.ROOT_ALLOCATOR_NAME;
            }


        });
    }

}
