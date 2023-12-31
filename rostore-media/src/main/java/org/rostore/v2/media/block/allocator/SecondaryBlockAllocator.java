package org.rostore.v2.media.block.allocator;

import org.rostore.entity.MemoryAllocation;
import org.rostore.entity.MemoryAllocationState;
import org.rostore.entity.QuotaExceededException;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.catalog.CachedCatalogBlockOperations;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.catalog.CatalogBlockOperations;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.BlockProviderImpl;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.container.Status;

public class SecondaryBlockAllocator {

    public static BlockAllocator create(final String allocatorName, final BlockAllocator rootBlockAllocator, final long upperBlockNumberLimit) {
        final CatalogBlockIndices catalogBlockIndices = rootBlockAllocator.allocate(BlockType.CATALOG, org.rostore.v2.seq.Properties.AVG_FREE_BLOCK_NUMBER);
        final BlockProviderImpl secondaryBlockProvider = BlockProviderImpl.internal(rootBlockAllocator);
        final CatalogBlockOperations reservedBlocksOperations = CatalogBlockOperations.create(secondaryBlockProvider, catalogBlockIndices);
        final BlockAllocator secondaryBlockAllocator = createSecondaryBlockAllocator(allocatorName, rootBlockAllocator, reservedBlocksOperations, upperBlockNumberLimit);
        secondaryBlockProvider.exchangeBlockAllocator(secondaryBlockAllocator);
        reservedBlocksOperations.add(catalogBlockIndices, true);
        reservedBlocksOperations.commit();
        if (reservedBlocksOperations.getBlockProvider().getMedia().getBlockAllocatorListeners().isEnabled()) {
            reservedBlocksOperations.getBlockProvider().getMedia().getBlockAllocatorListeners().
                    notifyAllocated(allocatorName, BlockType.CATALOG, catalogBlockIndices, true);
        }
        return secondaryBlockAllocator;
    }

    public static BlockAllocator load(final String allocatorName, final BlockAllocator rootBlockAllocator, long startIndex, long upperBlockNumberLimit) {
        final BlockProviderImpl secondaryBlockProvider = BlockProviderImpl.internal(rootBlockAllocator);
        final CatalogBlockOperations reservedBlocksOperations = CatalogBlockOperations.load(secondaryBlockProvider, startIndex);
        final BlockAllocator secondaryBlockAllocator = createSecondaryBlockAllocator(allocatorName, rootBlockAllocator, reservedBlocksOperations, upperBlockNumberLimit);
        secondaryBlockProvider.exchangeBlockAllocator(secondaryBlockAllocator);
        return secondaryBlockAllocator;
    }

    private static BlockAllocator createSecondaryBlockAllocator(final String name, final BlockAllocator rootBlockAllocator,
                                                           final CatalogBlockOperations _reservedBlocksOperations,
                                                           final long upperBlockNumberLimit) {
        final BlockAllocatorListeners blockAllocatorListeners = _reservedBlocksOperations.getBlockProvider().getMedia().getBlockAllocatorListeners();
        return BlockAllocator.wrap(new BlockAllocatorInternal() {

            private CachedCatalogBlockOperations reservedBlocksOperations = new CachedCatalogBlockOperations(_reservedBlocksOperations,
                    org.rostore.v2.media.block.allocator.Properties.SECONDARY_ALLOCATOR_CACHE_MIN_SIZE,
                    org.rostore.v2.media.block.allocator.Properties.SECONDARY_ALLOCATOR_CACHE_MAX_SIZE);

            @Override
            public MemoryAllocation getMemoryAllocation() {
                long payloadBlocks = reservedBlocksOperations.getAddedNumber() * getMedia().getMediaProperties().getBlockSize();
                long lockedFreeBlocks = reservedBlocksOperations.getCachedBlockNumber() * getMedia().getMediaProperties().getBlockSize();
                return MemoryAllocationState.init(0,
                        lockedFreeBlocks, payloadBlocks);
            }

            @Override
            public synchronized long getStartIndex() {
                return reservedBlocksOperations.getStartIndex();
            }

            @Override
            public Media getMedia() {
                return rootBlockAllocator.getMedia();
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public synchronized long getFreeBlocks() {
                checkOpened();
                if (upperBlockNumberLimit == 0) {
                    return rootBlockAllocator.getFreeBlocks();
                }
                return upperBlockNumberLimit - (reservedBlocksOperations.getAddedNumber() + reservedBlocksOperations.getSequenceIndexFreeBlockNumber() + reservedBlocksOperations.getCachedBlockNumber());
            }

            @Override
            public synchronized CatalogBlockIndices allocate(final BlockType blockType, final int blockNumber, final boolean rebalance) {
                checkOpened();
                checkFree(this, blockNumber);
                CatalogBlockIndices allocated = rootBlockAllocator.getBlockAllocatorInternal().allocate(blockType, blockNumber, rebalance);
                reservedBlocksOperations.add(allocated, rebalance);
                reservedBlocksOperations.commit();
                if (blockAllocatorListeners.isEnabled()) {
                    blockAllocatorListeners.notifyAllocated(getName(), blockType, allocated, rebalance);
                }
                return allocated;
            }

            @Override
            public synchronized long allocate(final BlockType blockType, final boolean rebalance) {
                checkOpened();
                checkFree(this, 1);
                long allocated = rootBlockAllocator.getBlockAllocatorInternal().allocate(blockType, rebalance);
                CatalogBlockIndices indices = new CatalogBlockIndices();
                indices.add(allocated, allocated);
                reservedBlocksOperations.add(indices, rebalance);
                reservedBlocksOperations.commit();
                if (blockAllocatorListeners.isEnabled()) {
                    blockAllocatorListeners.notifyAllocated(getName(), blockType, indices, rebalance);
                }
                return allocated;
            }

            @Override
            public synchronized void free(final long blockIndex, final boolean rebalance) {
                CatalogBlockIndices catalogBlockIndices = new CatalogBlockIndices();
                catalogBlockIndices.add(blockIndex, blockIndex);
                free(catalogBlockIndices, rebalance);
            }

            @Override
            public synchronized void free(final CatalogBlockIndices indices, final boolean rebalance) {
                checkOpened();
                reservedBlocksOperations.remove(indices, rebalance);
                reservedBlocksOperations.commit();
                rootBlockAllocator.getBlockAllocatorInternal().free(indices, rebalance);
                if (blockAllocatorListeners.isEnabled()) {
                    blockAllocatorListeners.notifyFreed(getName(), indices, rebalance);
                }
            }

            @Override
            public void dump() {
                System.out.println("Root:");
                rootBlockAllocator.getBlockAllocatorInternal().dump();
                System.out.println("Reserved:");
                reservedBlocksOperations.dump();
            }

            private void checkFree(final BlockAllocatorInternal blockAllocator, long requested) {
                final long tolerance = Properties.SECONDARY_ALLOCATOR_MIN_BUFFER;
                if (blockAllocator.getFreeBlocks() - tolerance < requested) {
                    throw new QuotaExceededException("Can't allocate " + requested + " blocks. No enough free blocks (" + blockAllocator.getFreeBlocks() + "-" + tolerance + ").");
                }
            }

            @Override
            public synchronized void close() {
                checkOpened();
                reservedBlocksOperations.close();
                reservedBlocksOperations.getBlockProvider().getBlockContainer().close();
            }

            @Override
            public Status getStatus() {
                return reservedBlocksOperations.getStatus();
            }

            @Override
            public synchronized void remove() {
                reservedBlocksOperations.iterateAll(catalogBlockIndices -> {
                    if (blockAllocatorListeners.isEnabled()) {
                        blockAllocatorListeners.notifyFreed(name, catalogBlockIndices, true);
                    }
                    rootBlockAllocator.free(catalogBlockIndices);
                });
                reservedBlocksOperations.getBlockProvider().getBlockContainer().close();
            }
        });
    }

}
