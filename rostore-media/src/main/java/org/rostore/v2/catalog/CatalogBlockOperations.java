package org.rostore.v2.catalog;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.fixsize.FixSizeEntryBlock;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.container.Status;
import org.rostore.v2.seq.BlockIndexSequence;
import org.rostore.v2.seq.BlockSequence;
import org.rostore.v2.seq.SequenceBlock;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This is the major class to manage a catalog of blocks.
 * This catalogs stores in a sequence of blocks the references to other blocks.
 * Can be used for example, to store a set of blocks for the secondary block allocator or
 * any other purpose of the persistent set of blocks.
 * <p>Has to be closed at the end</p>
 * <p>The provided block provider should be processed independently</p>
 */
public class CatalogBlockOperations implements Committable {

    private final FixSizeEntryBlock<CatalogBlockEntry> catalogBlock;
    private final CatalogBlockEntry catalogBlockEntry;

    /**
     * Provides a number of free blocks in the underlying {@link BlockIndexSequence}.
     * It is not the same as the free blocks that might be managed by the catalog itself.
     *
     * @return the number of free blocks
     */
    public long getSequenceIndexFreeBlockNumber() {
        return catalogBlock.getBlockSequence().getBlockIndexSequence().getFreeBlockNumber();
    }

    /**
     * A block provider that is used to manage the blocks in this catalog operations
     *
     * @return the block provider
     */
    public BlockProvider getBlockProvider() {
        return catalogBlock.getBlockProvider();
    }

    /**
     * Gives the index of the first block of the sequence where the data associated with catalog is stored.
     *
     * <p>It is enough to store this index somewhere to be able to restore the whole catalog</p>
     *
     * @return the first block index of this catalog
     */
    public long getStartIndex() {
        return catalogBlock.getBlockSequence().getBlockIndexSequence().getBlockIndex(0);
    }

    /**
     * Counts the rebalance operations (happens after the extraction or addition) -> write operation
     */
    private boolean collapseNeeded = false;

    private boolean rebalanceNeeded = false;

    /**
     * A function will iteratively provide all block indices in the catalog
     *
     * @param entryConsumer a consumer that would have to accept all the blocks
     */
    public void iterateAll(final Consumer<CatalogBlockIndices> entryConsumer) {
        checkOpened();
        catalogBlock.root();
        catalogBlockEntry.first();
        CatalogBlockIndices catalogBlockIndices = new CatalogBlockIndices();
        while (catalogBlockEntry.valid()) {
            catalogBlockIndices.add(catalogBlockEntry.getEntryStart(), catalogBlockEntry.getEntryStop());
            catalogBlockEntry.next();
            if (catalogBlockEntry.invalid()) {
                if (!catalogBlockIndices.isEmpty()) {
                    entryConsumer.accept(catalogBlockIndices);
                    catalogBlockIndices = new CatalogBlockIndices();
                }
                catalogBlock.next();
                if (catalogBlock.valid() && catalogBlock.getEntriesNumber() != 0) {
                    catalogBlockEntry.first();
                }
            }
        }
    }

    /**
     * Loads an instance of the catalog based on the index of the first block (given by {@link #getStartIndex()}).
     * @param blockProvider the block provider to be used by the object
     * @param startIndex the index of the first block
     * @return an instance of the catalog transaction
     */
    public static CatalogBlockOperations load(final BlockProvider blockProvider, final long startIndex) {
        return new CatalogBlockOperations(cbo -> SequenceBlock.load(blockProvider, startIndex,
                (Function<BlockSequence<FixSizeEntryBlock>, FixSizeEntryBlock>) sequence ->
                        new FixSizeEntryBlock(sequence,
                                blockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex(),
                                (Function<FixSizeEntryBlock<CatalogBlockEntry>, CatalogBlockEntry>)b -> new CatalogBlockEntry(b),
                                cbo::calculateCollapseNeeded), BlockType.CATALOG));
    }

    /**
     * Creates a new catalog.
     *
     * <p>After the catalog is created it can be stored just by storing {@link #getStartIndex()}, and
     * the next time it can be reinitialized by {@link #load(BlockProvider, long)}</p>
     *
     * <p>As the catalogs might participate in the allocators, they usually would require to
     * hold some free blocks to cover the internal allocation cycles, that's why the method requires
     * a set of blocks to initialize a new catalog.</p>
     *
     * @param blockProvider the block provider to be used
     * @param catalogBlockIndices this is the initial set of blocks that should be reserved for the underlying block sequence
     * @return an instance of the catalog
     */
    public static CatalogBlockOperations create(final BlockProvider blockProvider, CatalogBlockIndices catalogBlockIndices) {
        return new CatalogBlockOperations(cbo -> SequenceBlock.create(blockProvider,
                catalogBlockIndices,
                (Function<BlockSequence<FixSizeEntryBlock>, FixSizeEntryBlock>) sequence ->
                        new FixSizeEntryBlock(sequence,
                                blockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex(),
                                (Function<FixSizeEntryBlock<CatalogBlockEntry>, CatalogBlockEntry>)b -> new CatalogBlockEntry(b),
                                cbo::calculateCollapseNeeded), BlockType.CATALOG));
    }

    /**
     * Creates a new catalog.
     *
     * <p>After the catalog is created it can be stored just by storing {@link #getStartIndex()}, and
     * the next time it can be reinitialized by {@link #load(BlockProvider, long)}</p>
     *
     * @param blockProvider the block provider to be used
     * @return an instance of the catalog
     */
    public static CatalogBlockOperations create(final BlockProvider blockProvider) {
        return new CatalogBlockOperations(cbo -> SequenceBlock.create(blockProvider,
                (Function<BlockSequence<FixSizeEntryBlock>, FixSizeEntryBlock>) sequence ->
                        new FixSizeEntryBlock(sequence,
                                blockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex(),
                                (Function<FixSizeEntryBlock<CatalogBlockEntry>, CatalogBlockEntry>)b -> new CatalogBlockEntry(b),
                                cbo::calculateCollapseNeeded), BlockType.CATALOG));
    }

    private void calculateCollapseNeeded(final long newEntrySize, final long delta) {
        if (!catalogBlock.isRoot() && delta < 0 && newEntrySize < catalogBlock.getEntryCapacity() / 2) {
            collapseNeeded = true;
        }
    }

    private CatalogBlockOperations(final Function<CatalogBlockOperations, FixSizeEntryBlock<CatalogBlockEntry>> catalogBlockFactory) {
        catalogBlock = catalogBlockFactory.apply(this);
        catalogBlockEntry = catalogBlock.getEntry();
    }

    public void dump() {
        catalogBlock.root();
        while(catalogBlock.valid()) {
            if (catalogBlock.isRoot()) {
                System.out.println(catalogBlock.getBlock().getAbsoluteIndex() + ":" + catalogBlock.getEntriesNumber() + " of " + catalogBlock.getEntryCapacity() + ", added=" + catalogBlockEntry.getAddedNumber());
            } else {
                System.out.println(catalogBlock.getBlock().getAbsoluteIndex() + ":" + catalogBlock.getEntriesNumber() + " of " + catalogBlock.getEntryCapacity());
            }
            for(int i=0; i<catalogBlock.getEntriesNumber(); i++) {
                catalogBlockEntry.moveTo(i);
                System.out.println(" " + catalogBlockEntry.getIndex() + ": " + catalogBlockEntry.getEntryStart() + " -> " + catalogBlockEntry.getEntryStop());
            }
            catalogBlock.next();
        }
        catalogBlock.last();
        int lastActiveIndex = catalogBlock.getIndex();
        BlockIndexSequence blockIndexSequence = catalogBlock.getBlockSequence().getBlockIndexSequence();
        for(int i = lastActiveIndex + 1; i<blockIndexSequence.length(); i++) {
            System.out.println(blockIndexSequence.getBlockIndex(i) + ": free");
        }
    }

    private void incAddedNumber(final long added) {
        catalogBlock.root();
        catalogBlockEntry.incAddedNumber(added);
    }

    /**
     * Catalog counts and persists internally the total number of blocks
     * in the catalog.
     * @return the total number of blocks in the catalog
     */
    public long getAddedNumber() {
        checkOpened();
        catalogBlock.root();
        return catalogBlockEntry.getAddedNumber();
    }

    /**
     * This function will extract the requested number of blocks from the catalog.
     * It iterates from the bottom - up. And adds the blocks found in the catalog,
     * remove them from the catalog and add them to the returned {@link CatalogBlockIndices}.
     *
     * <p>Note that if the catalog will be empty at some point, it will return less blocks
     * than requested.</p>
     *
     * <p>With regards to rebalance, see {@link BlockSequence#rebalance()}</p>
     *
     * @param number the requested number of blocks
     * @param rebalance indicates if the rebalance should be executed
     * @return the set blocks that was available
     */
    public CatalogBlockIndices extractIndex(final long number, final boolean rebalance) {
        checkOpened();
        CatalogBlockIndices indices = new CatalogBlockIndices();
        catalogBlock.last();
        while (indices.getLength() != number && catalogBlock.getEntriesNumber() != 0) {
            // there are some entries in the last
            catalogBlockEntry.last();
            long entryBlockNumber = catalogBlockEntry.getBlocksNumber();
            long left = number - indices.getLength();
            if (entryBlockNumber <= left) {
                // this is the last entry in the catalog-block
                indices.add(catalogBlockEntry.getEntryStart(), catalogBlockEntry.getEntryStop());
                catalogBlockEntry.remove();
                if (!catalogBlock.isRoot()) {
                    if (catalogBlock.getEntriesNumber() == 0) {
                        rebalanceNeeded = true;
                        catalogBlock.delete();
                    }
                }
                catalogBlock.last();
            } else {
                entryBlockNumber = left;
                indices.add(catalogBlockEntry.getEntryStop()-left+1, catalogBlockEntry.getEntryStop());
                catalogBlockEntry.setEntryStop(catalogBlockEntry.getEntryStop()-left);
            }
            incAddedNumber(-entryBlockNumber);
            rebalance(rebalance);
            catalogBlock.last();
        }
        return indices;
    }

    private void rebalance(final boolean rebalance) {
        if (collapseNeeded) {
            collapse();
            collapseNeeded = false;
        }
        if (rebalanceNeeded && rebalance) {
            catalogBlock.getBlockSequence().rebalance();
            rebalanceNeeded=false;
        }
    }

    /**
     * The function removes gaps in the catalog blocks
     */
    private void collapse() {
        catalogBlock.root();
        while(catalogBlock.valid()) {
            int tolerance = catalogBlock.getEntryCapacity() / 2;
            int prevEntryCapacityLeft = catalogBlock.getEntryCapacity() - catalogBlock.getEntriesNumber();
            catalogBlock.next();
            if (!catalogBlock.valid()) {
                return;
            }
            int nextEntryNumber = catalogBlock.getEntriesNumber();
            // if after collapsing the free space in the target block will be greater than half of its capacity...
            if (prevEntryCapacityLeft - nextEntryNumber >= tolerance) {
                // they can be collapsed
                catalogBlock.previous();
                catalogBlock.moveEntriesFrom(catalogBlock.getIndex()+1, 0);
                rebalanceNeeded = true;
                catalogBlock.delete();
            }
        }
    }

    /**
     * Positions the catalogBlock & catalogBlockEntry at the entry
     * that points to block that just after the given blockIndex
     *
     * @param stopBlockIndex the stop block to locate
     */
    private void searchAfterBlockOrBlock(final long stopBlockIndex) {
        catalogBlock.root();
        if (catalogBlock.getEntriesNumber() == 0 && catalogBlock.getBlockSequence().length() != 1) {
            catalogBlock.next();
        }
        catalogBlockEntry.last();
        if (catalogBlockEntry.invalid()) {
            return;
        }
        if (stopBlockIndex > catalogBlockEntry.getEntryStop()) {
            // it can't be a root
            int startIndex = 0;
            // but should be at least the last one
            int stopIndex = catalogBlock.getBlockSequence().length() - 1;
            if (startIndex != stopIndex) {
                while (stopIndex - startIndex != 1) {
                    int next = (startIndex + stopIndex) / 2;
                    catalogBlock.moveTo(next);
                    catalogBlockEntry.last();
                    if (stopBlockIndex <= catalogBlockEntry.getEntryStop()) {
                        stopIndex = next;
                    } else {
                        startIndex = next;
                    }
                }
            }
            catalogBlock.moveTo(stopIndex);
        }
        searchInBlock(stopBlockIndex);
    }

    private void searchInBlock(final long stopBlockIndex) {
        // search a point somewhere in the selected block
        catalogBlockEntry.first();
        if (stopBlockIndex > catalogBlockEntry.getEntryStop()) {
            int startIndex = 0;
            int stopIndex = catalogBlock.getEntriesNumber() - 1;
            if (startIndex == stopIndex) {
                return;
            }
            while (stopIndex - startIndex != 1) {
                int next = (startIndex + stopIndex) / 2;
                catalogBlockEntry.moveTo(next);
                if (stopBlockIndex <= catalogBlockEntry.getEntryStop()) {
                    stopIndex = next;
                } else {
                    startIndex = next;
                }
            }
            catalogBlockEntry.moveTo(stopIndex);
        }
    }

    /**
     * Adds a set of blocks to the catalog.
     *
     * <p>With regards to rebalance, see {@link BlockSequence#rebalance()}</p>
     *
     * @param catalogBlockIndices the set of blocks
     *
     * @param rebalance indicates if the rebalance should be executed
     */
    public void add(final CatalogBlockIndices catalogBlockIndices, final boolean rebalance) {
        checkOpened();
        for(int i=0; i<catalogBlockIndices.getGroupNumber(); i++) {
            add(catalogBlockIndices.getGroup(i)[0], catalogBlockIndices.getGroup(i)[1], rebalance);
        }
    }

    /**
     * Removes a set of blocks from the catalog.
     *
     * <p>If some blocks won't found in the catalog - it would throw an exception.</p>
     *
     * <p>With regards to rebalance, see {@link BlockSequence#rebalance()}</p>
     *
     * @param catalogBlockIndices the set of blocks
     *
     * @param rebalance indicates if the rebalance should be executed
     */
    public void remove(final CatalogBlockIndices catalogBlockIndices, final boolean rebalance) {
        checkOpened();
        for(int i=0; i<catalogBlockIndices.getGroupNumber(); i++) {
            remove(catalogBlockIndices.getGroup(i)[0], catalogBlockIndices.getGroup(i)[1], rebalance);
        }
    }

    /**
     * Adds a set of blocks from the range {@param startIndex} - {@param stopIndex } to the catalog.
     *
     * <p>With regards to rebalance, see {@link BlockSequence#rebalance()}</p>
     *
     * @param startIndex the first block index to add
     * @param stopIndex the last block index to add
     * @param rebalance indicates if the rebalance should be executed
     */
    public void add(final long startIndex, final long stopIndex, final boolean rebalance) {
        checkOpened();
        if (stopIndex < startIndex) {
            throw new RoStoreException("Try to add an inconsistent block: " + startIndex + ".." + stopIndex);
        }

        if (addEdgeCases(startIndex, stopIndex)) {
            rebalance(rebalance);
            return;
        }

        searchAfterBlockOrBlock(stopIndex);
        if (stopIndex >= catalogBlockEntry.getEntryStart() && stopIndex <= catalogBlockEntry.getEntryStop() ||
                startIndex >= catalogBlockEntry.getEntryStart() && startIndex <= catalogBlockEntry.getEntryStop()) {
            throw new RoStoreException("The blocks " + startIndex + ".." + stopIndex + " have already been added to catalogue.");
        }

        final CatalogBlockEntryInstance after = new CatalogBlockEntryInstance(catalogBlockEntry, catalogBlockEntry.getHash());
        if (catalogBlockEntry.isFirst()) {
            catalogBlock.previous();
            catalogBlockEntry.last();
        } else {
            catalogBlockEntry.previous();
        }
        if (stopIndex >= catalogBlockEntry.getEntryStart() && stopIndex <= catalogBlockEntry.getEntryStop() ||
                startIndex >= catalogBlockEntry.getEntryStart() && startIndex <= catalogBlockEntry.getEntryStop()) {
            throw new RoStoreException("The blocks " + startIndex + ".." + stopIndex + " have already been added to catalogue.");
        }
        final CatalogBlockEntryInstance before = new CatalogBlockEntryInstance(catalogBlockEntry, catalogBlockEntry.getHash());

        if (after.getStart() == stopIndex + 1 && before.getStop() == startIndex - 1) {
            // insert is a union
            before.restore();
            catalogBlockEntry.setEntryStop(after.getStop());
            after.restore();
            catalogBlockEntry.remove();
            if (catalogBlock.getEntriesNumber()==0) {
                // after the after id removed the block is empty
                if (!catalogBlock.isRoot()) {
                    rebalanceNeeded = true;
                    catalogBlock.delete();
                }
            }
        } else {
            if (after.getStart() == stopIndex + 1) {
                after.restore();
                catalogBlockEntry.setEntryStart(startIndex);
            } else {
                if (before.getStop() == startIndex - 1) {
                    before.restore();
                    catalogBlockEntry.setEntryStop(stopIndex);
                } else {
                    after.restore();
                    if (catalogBlock.hasFreeSpace()) {
                        catalogBlockEntry.insert();
                        catalogBlockEntry.setEntryStartStop(startIndex, stopIndex);
                    } else {
                        before.restore();
                        if (catalogBlock.hasFreeSpace()) {
                            catalogBlockEntry.expand();
                            catalogBlockEntry.setEntryStartStop(startIndex, stopIndex);
                        } else {
                            // no space in before & after, expansion
                            insertAfter(startIndex, stopIndex);
                        }
                    }
                }
            }
        }
        incAddedNumber(stopIndex - startIndex + 1);
        rebalance(rebalance);
    }

    /**
     * Removes a set of blocks from the range {@param startIndex} - {@param stopIndex } from the catalog.
     *
     * <p>If some blocks won't found in the catalog - it would throw an exception.</p>
     * <p>With regards to rebalance, see {@link BlockSequence#rebalance()}</p>
     *
     * @param startIndex the first block index to remove
     * @param stopIndex the last block index to remove
     * @param rebalance indicates if the rebalance should be executed
     */
    public void remove(final long startIndex, final long stopIndex, final boolean rebalance) {

        checkOpened();

        if (stopIndex < startIndex) {
            throw new RoStoreException("Try to remove an inconsistent block: " + startIndex + ".." + stopIndex);
        }
        searchAfterBlockOrBlock(stopIndex);
        if (catalogBlockEntry.invalid() || startIndex < catalogBlockEntry.getEntryStart() || stopIndex > catalogBlockEntry.getEntryStop()) {
            throw new RoStoreException("The block " + startIndex + ".." + stopIndex + " is not consistent with catalog structure.");
        }
        if (catalogBlockEntry.getEntryStart() == startIndex) {
            if (catalogBlockEntry.getEntryStop() == stopIndex) {
                catalogBlockEntry.remove();
                if (catalogBlock.getEntriesNumber()==0) {
                    // after the after id removed the block is empty
                    if (!catalogBlock.isRoot()) {
                        rebalanceNeeded = true;
                        catalogBlock.delete();
                    }
                }
            } else {
                catalogBlockEntry.setEntryStart(stopIndex+1);
            }
        } else {
            if (catalogBlockEntry.getEntryStop() == stopIndex) {
                catalogBlockEntry.setEntryStop(startIndex-1);
            } else {
                // in the middle
                long newEntryStop = catalogBlockEntry.getEntryStop();
                catalogBlockEntry.setEntryStop(startIndex-1);
                long newEntryStart = stopIndex+1;
                if (catalogBlock.hasFreeSpace()) {
                    catalogBlockEntry.next();
                    if (catalogBlockEntry.invalid()) {
                        catalogBlockEntry.expand();
                    } else {
                        catalogBlockEntry.insert();
                    }
                    catalogBlockEntry.setEntryStartStop(newEntryStart, newEntryStop);
                } else {
                    // no space in the current
                    catalogBlockEntry.next();
                    if (catalogBlockEntry.invalid()) {
                        // this is the last entry in the block
                        catalogBlock.next();
                        if (catalogBlock.valid()) {
                            // there is a next block
                            if (catalogBlock.hasFreeSpace()) {
                                catalogBlockEntry.first();
                                catalogBlockEntry.insert();
                                catalogBlockEntry.setEntryStartStop(newEntryStart, newEntryStop);
                            } else {
                                catalogBlock.previous();
                                catalogBlock.createNewAfter();
                                catalogBlockEntry.addNewEntryFor(newEntryStart, newEntryStop);
                            }
                        } else {
                            // this is really the last block and the last entry
                            catalogBlock.last();
                            catalogBlock.createNewAfter();
                            catalogBlockEntry.addNewEntryFor(newEntryStart, newEntryStop);
                        }
                    } else {
                        // there are some follow-up entries
                        catalogBlockEntry.previous();
                        insertAfter(newEntryStart, newEntryStop);
                    }
                }
            }
        }
        incAddedNumber(-(stopIndex - startIndex + 1));
        rebalance(rebalance);
    }

    /**
     * Frees a block that is before the root or after the last
     *
     * @param startIndex the start index
     * @param stopIndex the stop index
     * @return true if the case has been processed as an edge case
     */
    private boolean addEdgeCases(final long startIndex, final long stopIndex) {
        catalogBlock.root();
        if (catalogBlock.getEntriesNumber()==0) {
            // there is no any records in the root
            if (catalogBlock.getBlockSequence().length() == 1) {
                catalogBlock.root();
                catalogBlockEntry.expand();
                catalogBlockEntry.setEntryStartStop(startIndex, stopIndex);
                incAddedNumber(stopIndex - startIndex + 1);
                return true;
            }
            // more than just one entry
            catalogBlock.next();
            catalogBlockEntry.first();
            if (stopIndex < catalogBlockEntry.getEntryStart()) {
                // insert as a first entry in the root
                if (catalogBlockEntry.getEntryStart() == stopIndex+1) {
                    catalogBlockEntry.setEntryStart(startIndex);
                    incAddedNumber(stopIndex-startIndex+1);
                    return true;
                }
                catalogBlock.root();
                catalogBlockEntry.expand();
                catalogBlockEntry.setEntryStartStop(startIndex, stopIndex);
                incAddedNumber(stopIndex-startIndex+1);
                return true;
            }
        }
        catalogBlockEntry.first();
        if (stopIndex < catalogBlockEntry.getEntryStart()) {
            // insert as a first entry in the root
            if (catalogBlockEntry.getEntryStart() == stopIndex+1) {
                catalogBlockEntry.setEntryStart(startIndex);
                incAddedNumber(stopIndex-startIndex+1);
                return true;
            }
            if (catalogBlock.hasFreeSpace()) {
                catalogBlockEntry.insert();
                catalogBlockEntry.setEntryStartStop(startIndex,stopIndex);
                incAddedNumber(stopIndex-startIndex+1);
                return true;
            }
            // no free space in the root...
            int numberToStart = catalogBlock.getEntriesNumber() / 2;
            rebalanceNeeded = true;
            catalogBlock.createNewAfter();
            catalogBlock.moveEntriesFrom(0, numberToStart);
            catalogBlock.root();
            catalogBlockEntry.insert();
            catalogBlockEntry.setEntryStartStop(startIndex, stopIndex);
            incAddedNumber(stopIndex-startIndex+1);
            return true;
        }
        catalogBlock.last();
        catalogBlockEntry.last();
        if (startIndex > catalogBlockEntry.getEntryStop()) {
            // this entry comes at the very end
            // no after, meaning we are at the end
            if (catalogBlockEntry.getEntryStop() == startIndex - 1) {
                catalogBlockEntry.setEntryStop(stopIndex);
                incAddedNumber(stopIndex-startIndex+1);
                return true;
            }

            if (catalogBlock.hasFreeSpace()) {
                catalogBlockEntry.expand();
                catalogBlockEntry.setEntryStartStop(startIndex, stopIndex);
                incAddedNumber(stopIndex-startIndex+1);
                return true;
            }
            // no free space in the last entry
            rebalanceNeeded = true;
            catalogBlock.createNewAfter();
            catalogBlockEntry.addNewEntryFor(startIndex, stopIndex);
            incAddedNumber(stopIndex-startIndex+1);
            return true;
        }
        return false;
    }

    private void insertAfter(final long startIndex, final long stopIndex)
    {
        int beforeBlockIndex = catalogBlock.getIndex();
        int moveStartIndex = catalogBlockEntry.getIndex()+1;
        if (moveStartIndex >= catalogBlock.getEntriesNumber()) {
            moveStartIndex = -1;
        }
        rebalanceNeeded = true;
        catalogBlock.createNewAfter();
        catalogBlockEntry.addNewEntryFor(startIndex, stopIndex);
        if (moveStartIndex!=-1) {
            catalogBlock.moveEntriesFrom(beforeBlockIndex, moveStartIndex);
        }
    }

    /**
     * Closes the catalog and frees the sequence,
     * blocks still needs to be committed/closed by the usage of {@link #getBlockProvider()}
     */
    @Override
    public void close() {
        commit();
        catalogBlock.getBlockSequence().close();
    }

    @Override
    public Status getStatus() {
        return catalogBlock.getStatus();
    }

    /**
     * Commit all the active blocks from the associated container
     */
    @Override
    public void commit() {
        catalogBlock.getBlockProvider().getBlockContainer().commit();
    }
}

