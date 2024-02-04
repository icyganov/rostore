package org.rostore.v2.media.block.allocator;

import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.catalog.CatalogBlockIndicesIterator;
import org.rostore.v2.media.block.BlockType;

import java.util.HashMap;
import java.util.Map;

/**
 * Special kind of listener that can be used to verify the block allocations
 * <p>It is a debugging tool.</p>
 */
public class BlockVerifierListener implements BlockAllocatorListener {

    private final Map<String, Map<Long, BlockType>> allocated;

    public BlockVerifierListener() {
        allocated = new HashMap<>();
    }

    private BlockVerifierListener(Map<String, Map<Long, BlockType>> allocated) {
        this.allocated = allocated;
    }

    public BlockVerifierListener snapshot() {
        final Map<String, Map<Long, BlockType>> copy = new HashMap<>();
        for(Map.Entry<String, Map<Long, BlockType>> entry : allocated.entrySet()) {
            Map<Long, BlockType> b = new HashMap<>();
            copy.put(entry.getKey(), b);
            for(Map.Entry<Long, BlockType> entry2 : entry.getValue().entrySet()) {
                b.put(entry2.getKey(), entry2.getValue());
            }
        }
        return new BlockVerifierListener(copy);
    }

    @Override
    public synchronized void blocksFreed(final String name, CatalogBlockIndices catalogBlockIndices, boolean rebalance) {
        final Map<Long, BlockType> all = allocated.get(name);
        final CatalogBlockIndicesIterator iterator = catalogBlockIndices.iterator();
        while (iterator.isValid()) {
            long id = iterator.get();
            //System.out.println(name + ":free:" + id + ":" + all.get(id));
            if (all.remove(id) == null) {
                int z=0;
                // throw new IllegalStateException("The block id=" + id + " does not exist in the allocator=<" + name + ">");
            }
            //System.out.println(name + ":free:" + id);
        }
        if (all.isEmpty()) {
            //System.out.println("remove: " + name);
            allocated.remove(name);
        }
    }

    @Override
    public synchronized void blocksAllocated(String name, BlockType blockType, CatalogBlockIndices catalogBlockIndices, boolean rebalance) {
        Map<Long, BlockType> all = allocated.get(name);
        if (all == null) {
            all = new HashMap<>();
            allocated.put(name, all);
        }
        final CatalogBlockIndicesIterator iterator = catalogBlockIndices.iterator();
        while(iterator.isValid()) {
            long id = iterator.get();
            if (all.get(id) != null) {
                int z=0;
                //throw new IllegalStateException("The block id=" + id + " has already been allocated in the allocator=<" + name + ">");
            }
            all.put(id, blockType);
            //System.out.println(name + ":alloc:" + blockType + ":" + id);
        }
    }
}
