package org.rostore.v2.media.block;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.MediaProperties;
import org.rostore.v2.media.block.container.BlockContainer;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a container of all active physical blocks in the ro-store.
 * <p>It manages a cache of the blocks, so that different processes would get the
 * access to the same instances of the blocks.</p>
 * <p>When the blocks are not used by any processes they still be preserved in
 * the cache until the time provided in {@link MediaProperties#getCloseUnusedBlocksAfterMillis()} is expired.</p>
 * <p>This passive caching is only applied for the non-data blocks, the blocks which type
 * is not {@link BlockType#DATA}.</p>
 * <p>This is a major hub of blocks in the {@link Media} and this class manages the
 * concurrent access and is internally is thread safe.</p>
 * <p>A great precaution should be paid to the synchronized blocks in its implementation.</p>
 */
public class MappedPhysicalBlocks {

    private static final Logger logger = Logger.getLogger(Media.class.getName());

    private final Map<Long, MappedPhysicalBlock> active = new HashMap<>();
    private final Map<Long, MappedPhysicalBlock> passive = new LinkedHashMap<>();
    private final Media media;
    private int maxBlocks = 0;

    /**
     * Creates an object
     *
     * @param media the media it should be used in
     */
    public MappedPhysicalBlocks(final Media media) {
        this.media = media;
    }

    /**
     * The total size of passively cached blocks and the actively used that are
     * currently managed by this instance.
     *
     * @return a number of blocks currently in the memory
     */
    public int size() {
        return active.size() + passive.size();
    }

    /**
     * Creates a {@link BlockContainer} private {@link Block}. If the block is not in use or in cache,
     * it is get loaded from the persistence layer.
     *
     * @param blockContainer the container the block should be associated with
     * @param index the index of the block
     * @param blockType the type of the block
     * @return a block that can be used in the container
     */
    public Block get(final BlockContainer blockContainer, final long index, final BlockType blockType) {
        MappedPhysicalBlock mappedPhysicalBlock;
        synchronized (this) {
            mappedPhysicalBlock = active.get(index);
            if (mappedPhysicalBlock == null) {
                mappedPhysicalBlock = passive.remove(index);
                if (mappedPhysicalBlock == null) {
                    mappedPhysicalBlock = new MappedPhysicalBlock(index, blockType);
                    final int nextSize = size() + 1;
                    if (nextSize > maxBlocks) {
                        maxBlocks = nextSize;
                        if (maxBlocks % 10000 == 0) {
                            logger.log(Level.INFO, "Max number of physical blocks has been changed, maxBlocks={0}", maxBlocks);
                        }
                    }
                } else {
                    mappedPhysicalBlock.setBlockType(blockType);
                }
                active.put(index, mappedPhysicalBlock);
            } else {
                if (!blockType.equals(mappedPhysicalBlock.getBlockType())) {
                    final List<Integer> containerIds = new ArrayList<>(mappedPhysicalBlock.getAllContainerIds());
                    StringBuilder sb = new StringBuilder("Incompatible block (");
                    sb.append(index);
                    sb.append(") types: assigned=");
                    sb.append(mappedPhysicalBlock.getBlockType());
                    if (!containerIds.isEmpty()) {
                        sb.append(" in containers={");
                        for(int i=0; i<containerIds.size(); i++) {
                            if (i!=0) {
                                sb.append(",");
                            }
                            sb.append(containerIds.get(0));
                            sb.append("(");
                            sb.append(media.getBlockContainer(containerIds.get(0)).getStatus());
                            sb.append(")");
                        }
                        sb.append("}");
                    }
                    sb.append(", requested=");
                    sb.append(blockType);
                    throw new RoStoreException(sb.toString());
                }
            }
            mappedPhysicalBlock.markAsUsed(blockContainer);
        }
        return mappedPhysicalBlock.get(blockContainer);
    }

    public void remove(final BlockContainer blockContainer, final long index) {
        MappedPhysicalBlock mappedPhysicalBlock;
        synchronized (this) {
            mappedPhysicalBlock = active.get(index);
        }
        if (mappedPhysicalBlock != null) {
            mappedPhysicalBlock.flush();
            synchronized (this) {
                mappedPhysicalBlock.remove(blockContainer);
                if (!mappedPhysicalBlock.inUse()) {
                    active.remove(index);
                    if (!BlockType.DATA.equals(mappedPhysicalBlock.getBlockType())) {
                        passive.put(index, mappedPhysicalBlock);
                    }
                }
            }
        } else {
            throw new RoStoreException("Removing a non-active block " + index + " from container " + blockContainer.getContainerId());
        }
    }

    public synchronized void closeExpired() {
        long currentTime = System.currentTimeMillis();

        final List<Long> blockIdsToRemove = new ArrayList<>();
        for (final Map.Entry<Long, MappedPhysicalBlock> entry : passive.entrySet()) {
            if (currentTime - entry.getValue().getUnusedSince() > media.getMediaProperties().getCloseUnusedBlocksAfterMillis()) {
                blockIdsToRemove.add(entry.getKey());
            }
        }
        blockIdsToRemove.forEach(id -> passive.remove(id));
        if (!blockIdsToRemove.isEmpty()) {
            logger.log(Level.FINE, "Removed expired physical blocks: {0} ", blockIdsToRemove.size());
        }
    }

    public synchronized void closeUnused() {
        passive.clear();
    }

}
