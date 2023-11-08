package org.rostore.v2.catalog;

import org.rostore.v2.media.Committable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.container.Status;

import java.util.function.Consumer;

public class CachedCatalogBlockOperations implements Committable {

    final private CatalogBlockOperations catalogBlockOperations;
    final private int minCacheSize;
    final private int maxCacheSize;
    final private int avgCacheSize;

    final private CatalogBlockIndices cache;

    public long getSequenceIndexFreeBlockNumber() {
        return this.catalogBlockOperations.getSequenceIndexFreeBlockNumber();
    }

    public long getCachedBlockNumber() {
        return cache.getLength();
    }

    public BlockProvider getBlockProvider() {
        return catalogBlockOperations.getBlockProvider();
    }

    public CachedCatalogBlockOperations(final CatalogBlockOperations catalogBlockOperations,
                                        int minCacheSize,
                                        int maxCacheSize) {
        this.catalogBlockOperations = catalogBlockOperations;
        this.maxCacheSize = maxCacheSize;
        this.minCacheSize = minCacheSize;

        avgCacheSize = (maxCacheSize + minCacheSize) / 2;
        cache = new CatalogBlockIndices();
    }

    public long getAddedNumber() {
        return catalogBlockOperations.getAddedNumber();
    }

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

    public void add(final CatalogBlockIndices indices, boolean rebalance) {
        checkOpened();
        cache.add(indices);
        if (rebalance) {
            // cache is NOT refreshed on the rebalancing cycle to not saturate the free blocks
            if (cache.getLength() > maxCacheSize) {
                int cacheFreeSize = cache.getLength() - avgCacheSize;
                CatalogBlockIndices toReturn = cache.extract(cacheFreeSize);
                catalogBlockOperations.add(toReturn, rebalance);
            }
        }
    }

    public void remove(final CatalogBlockIndices indices, boolean rebalance) {
        checkOpened();
        final CatalogBlockIndices notRemoved = cache.remove(indices);
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

    @Override
    public void close() {
        catalogBlockOperations.add(cache, true);
        cache.clear();
        catalogBlockOperations.close();
    }

    @Override
    public Status getStatus() {
        return catalogBlockOperations.getStatus();
    }

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

    public long getStartIndex() {
        return catalogBlockOperations.getStartIndex();
    }

    public void remove() {
        catalogBlockOperations.remove();
    }

    public Media getMedia() {
        return catalogBlockOperations.getBlockProvider().getMedia();
    }

    public void iterateAll(final Consumer<CatalogBlockIndices> consumer) {
        consumer.accept(cache);
        catalogBlockOperations.iterateAll(consumer);
    }
}
