package org.rostore.v2.seq;

import org.rostore.v2.media.Media;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BlockIndexSequences {

    private static final Logger logger = Logger.getLogger(Media.class.getName());

    private final Map<Long, BlockIndexSequence> blockIndexSequences = new HashMap<>();
    private final Media media;

    public synchronized int size() {
        return blockIndexSequences.size();
    }

    public BlockIndexSequences(final Media media) {
        this.media = media;
    }

    public synchronized <T extends SequenceBlock> BlockSequence<T> get(final long startIndex, final Function<BlockIndexSequence, BlockSequence<T>> factory) {
        final BlockIndexSequence blockIndexSequence = blockIndexSequences.get(startIndex);
        if (blockIndexSequence != null) {
            return factory.apply(blockIndexSequence);
        }
        final BlockSequence<T> blockSequence = factory.apply(null);
        blockIndexSequences.put(blockSequence.getBlockIndexSequence().getBlockIndex(0), blockSequence.getBlockIndexSequence());
        return blockSequence;
    }

    public synchronized void closeExpired() {
        final long currentTime = System.currentTimeMillis();
        final List<Long> blockSeqIdsToRemove = filter((bis) ->
            bis.isSequenceInUse() ||
                    currentTime - bis.getLastUsageTimestampMillis() < media.getMediaProperties().getCloseUnusedSequencesAfterMillis());
        if (!blockSeqIdsToRemove.isEmpty()) {
            logger.log(Level.FINE, "Removed expired block sequences: " + blockSeqIdsToRemove.size());
        }
    }

    public List<Long> filter(final Function<BlockIndexSequence, Boolean> filter) {
        final List<Long> blockSeqIdsToRemove = new ArrayList<>();
        for (final Map.Entry<Long, BlockIndexSequence> entry : blockIndexSequences.entrySet()) {
            if (!filter.apply(entry.getValue())) {
                blockSeqIdsToRemove.add(entry.getKey());
            }
        }
        for (final long id : blockSeqIdsToRemove) {
            blockIndexSequences.remove(id);
        }
        return blockSeqIdsToRemove;
    }
}
