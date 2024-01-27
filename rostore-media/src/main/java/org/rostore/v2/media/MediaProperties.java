package org.rostore.v2.media;

import org.rostore.Utils;
import org.rostore.entity.media.MediaPropertiesBuilder;
import org.rostore.mapper.MapperProperties;

/**
 * Properties of the media that defines it major parameters.
 *
 * <p>This can't be changed after the media is created.</p>
 */
public class MediaProperties {

    private long maxTotalSize;
    private int blockSize;
    private long totalBlockNumber;
    private long closeUnusedBlocksAfterMillis;
    private long closeUnusedSequencesAfterMillis;
    private MapperProperties mapperProperties;

    public static MediaProperties from(final MediaPropertiesBuilder builder) {
        MediaProperties mediaProperties = new MediaProperties();
        mediaProperties.maxTotalSize = builder.getMaxTotalSize();
        mediaProperties.blockSize = builder.getBlockSize();
        mediaProperties.totalBlockNumber = mediaProperties.maxTotalSize / mediaProperties.blockSize;
        mediaProperties.closeUnusedSequencesAfterMillis = builder.getCloseUnusedSequencesAfterMillis();
        mediaProperties.closeUnusedBlocksAfterMillis = builder.getCloseUnusedBlocksAfterMillis();
        MapperProperties mapperProperties = new MapperProperties();
        mapperProperties.setBytesPerBlockIndex(Utils.computeBytesForMaxValue(builder.getMaxTotalSize() / builder.getBlockSize() + 1));
        mapperProperties.setBytesPerBlockOffset(Utils.computeBytesForMaxValue(builder.getBlockSize()-1));
        mediaProperties.mapperProperties = mapperProperties;
        return mediaProperties;
    }

    public long getTotalBlockNumber() {
        return totalBlockNumber;
    }

    public long getMaxTotalSize() {
        return maxTotalSize;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public long getCloseUnusedBlocksAfterMillis() {
        return closeUnusedBlocksAfterMillis;
    }

    public long getCloseUnusedSequencesAfterMillis() {
        return closeUnusedSequencesAfterMillis;
    }

    public MapperProperties getMapperProperties() {
        return mapperProperties;
    }
}
