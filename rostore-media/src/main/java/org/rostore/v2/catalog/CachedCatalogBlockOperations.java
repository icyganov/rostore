package org.rostore.v2.catalog;

import org.rostore.v2.media.Committable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.container.Status;
import org.rostore.v2.seq.BlockIndexSequence;
import org.rostore.v2.seq.BlockSequence;

import java.util.function.Consumer;

/**
 * This class extends {@link CatalogBlockOperations} by caching some subset
 * of the catalog in memory. It will make operations faster, but also
 * would make some operation less fault-tolerant.
 *
 * <p>When the catalog is created, a min and max size of the cache is provided.
 * if the internal cache become bigger than the given maximum, then the entries are
 * written to the catalog. If the cache size is lower than the minimum, the
 * values are extracted form the catalog.</p>
 *
 */
public class CachedCatalogBlockOperations implements Committable {

    final private CatalogBlockOperations catalogBlockOperations;
    final private int minCacheSize;
    final private int maxCacheSize;
    final private int avgCacheSize;

    final private CatalogBlockIndices cache;

    /**
     * Provides a number of free blocks in the underlying {@link BlockIndexSequence}.
     * It is not the same as the free blocks that might be managed by the catalog itself.
     *
     * @return the number of free blocks
     */
    public long getSequenceIndexFreeBlockNumber() {
        return this.catalogBlockOperations.getSequenceIndexFreeBlockNumber();
    }

    /**
     * Provides the number of blocks in the cache
     * @return the cache size
     */
    public long getCachedBlockNumber() {
        return cache.getLength();
    }

    /**
     * A block provider that is used to manage the blocks in this catalog operations
     *
     * @return the block provider
     */
    public BlockProvider getBlockProvider() {
        return catalogBlockOperations.getBlockProvider();
    }

    /**
     * Creates a cached version with the {@link CatalogBlockOperations} backend
     * @param catalogBlockOperations the backing catalog ops
     * @param minCacheSize the minimum size of the cache
     * @param maxCacheSize the maximum size of the cache
     */
    public CachedCatalogBlockOperations(final CatalogBlockOperations catalogBlockOperations,
                                        int minCacheSize,
                                        int maxCacheSize) {
        this.catalogBlockOperations = catalogBlockOperations;
        this.maxCacheSize = maxCacheSize;
        this.minCacheSize = minCacheSize;

        avgCacheSize = (maxCacheSize + minCacheSize) / 2;
        cache = new CatalogBlockIndices();
    }

    /**
     * Catalog counts and persists internally the total number of blocks
     * in the catalog.
     * @return the total number of blocks in the catalog
     */
    public long getAddedNumber() {
        return catalogBlockOperations.getAddedNumber();
    }

    /**
     * This is the version of {@link CatalogBlockOperations#extractIndex(long, boolean)} that uses internal cache
     *
     * @param blockNumber the number of blocks to extract
     * @param rebalance indicates if rebalance should be executed
     * @return the indices that has been extracted
     */
    public CatalogBlockIndices extractIndex(final int blockNumber, final boolean rebalance) {
        checkOpened();
        if (cache.getLength() >= blockNumber) {
            CatalogBlockIndices ret = cache.extract(blockNumber);
            if (rebalance) {
                // cache replenish only in case of rebalance...
                if (cache.getLength() < minCacheSize) {
                    final int cacheCapacity = avgCacheSize - cache.getLength();
                    CatalogBlockIndices extracted = catalogBlockOperations.extractIndex(cacheCapacity, rebalance);
                    cache.add(extracted);
                }
            }
            return ret;
        }

        if (rebalance) {
            // replenish cache and get as many as possible
            final int cacheCapacity = cache.getLength() < minCacheSize ? avgCacheSize - cache.getLength() : 0;
            final CatalogBlockIndices extracted = catalogBlockOperations.extractIndex(blockNumber + cacheCapacity, rebalance);

            if (cacheCapacity != 0) {
                final CatalogBlockIndices ret = extracted.extract(blockNumber);
                cache.add(extracted);
                return ret;
            }
            return extracted;
        } else {
            // do not replenish cache in rebalancing cycle
            int fromCache = cache.getLength();
            CatalogBlockIndices ret = cache.extract(fromCache);
            int left = blockNumber - fromCache;
            final CatalogBlockIndices extracted = catalogBlockOperations.extractIndex(left, rebalance);
            ret.add(extracted);
            return ret;
        }
    }

    /**
     * This is a version of {@link CatalogBlockOperations#add(CatalogBlockIndices, boolean)} with the cache
     *
     * <p>With regards to rebalance, see {@link BlockSequence#rebalance()}</p>
     *
     * @param catalogBlockIndices the set of blocks
     * @param rebalance indicates if the rebalance should be executed
     */
    public void add(final CatalogBlockIndices catalogBlockIndices, final boolean rebalance) {
        checkOpened();
        cache.add(catalogBlockIndices);
        if (rebalance) {
            // cache is NOT refreshed on the rebalancing cycle to not saturate the free blocks
            if (cache.getLength() > maxCacheSize) {
                int cacheFreeSize = cache.getLength() - avgCacheSize;
                CatalogBlockIndices toReturn = cache.extract(cacheFreeSize);
                catalogBlockOperations.add(toReturn, rebalance);
            }
        }
    }

    /**
     * This is a version of {@link CatalogBlockOperations#remove(CatalogBlockIndices, boolean)} with the cache
     *
     * <p>With regards to rebalance, see {@link BlockSequence#rebalance()}</p>
     *
     * @param catalogBlockIndices the set of blocks
     * @param rebalance indicates if the rebalance should be executed
     */
    public void remove(final CatalogBlockIndices catalogBlockIndices, final boolean rebalance) {
        checkOpened();
        final CatalogBlockIndices notRemoved = cache.remove(catalogBlockIndices);
        if (!notRemoved.isEmpty()) {
            catalogBlockOperations.remove(notRemoved, rebalance);
        }
        if (rebalance) {
            final int cacheCapacity = cache.getLength() < minCacheSize ? avgCacheSize - cache.getLength() : 0;
            if (cacheCapacity != 0) {
                final CatalogBlockIndices extracted = catalogBlockOperations.extractIndex(cacheCapacity, rebalance);
                cache.add(extracted);
            }
        }
    }

    /**
     * Returns all the entries from the cache back to catalog and closes it.
     */
    @Override
    public void close() {
        catalogBlockOperations.add(cache, true);
        cache.clear();
        catalogBlockOperations.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Status getStatus() {
        return catalogBlockOperations.getStatus();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() {
        catalogBlockOperations.commit();
    }

    public void dump() {
        System.out.println(">b---------------------------------");
        System.out.println("cache=" + cache.toString());
        catalogBlockOperations.dump();
        System.out.println("<e---------------------------------");
    }

    /**
     * Gives the index of the first block of the sequence where the data associated with catalog is stored.
     *
     * <p>It is enough to store this index somewhere to be able to restore the whole catalog</p>
     *
     * @return the first block index of this catalog
     */
    public long getStartIndex() {
        return catalogBlockOperations.getStartIndex();
    }

    /**
     * Provides the parent media
     *
     * @return the parent media
     */
    public Media getMedia() {
        return catalogBlockOperations.getBlockProvider().getMedia();
    }

    /**
     * Iterates all blocks in the catalog, inclusive those from the cache
     *
     * @param consumer the consumer to consume all the blocks
     */
    public void iterateAll(final Consumer<CatalogBlockIndices> consumer) {
        consumer.accept(cache);
        catalogBlockOperations.iterateAll(consumer);
    }
}
