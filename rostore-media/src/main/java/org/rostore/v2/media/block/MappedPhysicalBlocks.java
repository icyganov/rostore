package org.rostore.v2.media.block;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.container.BlockContainer;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MappedPhysicalBlocks {

    private static final Logger logger = Logger.getLogger(Media.class.getName());

    private final Map<Long, MappedPhysicalBlock> active = new HashMap<>();
    private final Map<Long, MappedPhysicalBlock> passive = new LinkedHashMap<>();
    private final Media media;
    private int maxBlocks = 0;

    public MappedPhysicalBlocks(final Media media) {
        this.media = media;
    }

    public int size() {
        return active.size() + passive.size();
    }

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
