package org.rostore.v2.media;

import org.rostore.Utils;
import org.rostore.entity.media.MediaPropertiesBuilder;
import org.rostore.mapper.MapperProperties;

/**
 * Properties of the media that defines its major parameters.
 *
 * <p>This is an immutable variant to prevent its changes after the media is created.</p>
 * <p>Use {@link MediaPropertiesBuilder} to populate the fields and then create this object.</p>
 */
public class MediaProperties {

    private long maxTotalSize;
    private int blockSize;
    private long totalBlockNumber;
    private long closeUnusedBlocksAfterMillis;
    private long closeUnusedSequencesAfterMillis;
    private MapperProperties mapperProperties;

    /**
     * Used to create the MediaProperties based on the desired input in form of {@link MediaPropertiesBuilder}.
     *
     * <p>Note that some fields might be corrected and deviate from the original.</p>
     *
     * @param builder the object with the data
     * @return a read-only copy of the builder's data
     */
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

    /**
     * Provides the total number of blocks that the storage supports
     *
     * @return the number of blocks
     */
    public long getTotalBlockNumber() {
        return totalBlockNumber;
    }

    /**
     * Provides the total maximum size of the storage
     *
     * @return the size in bytes
     */
    public long getMaxTotalSize() {
        return maxTotalSize;
    }

    /**
     * Provides the block size
     *
     * @return the block size in bytes
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Provides the time interval in milliseconds after each the unused blocks will
     * be removed from the memory.
     *
     * <p>The caching of unused block is only used fo the blocks of CATALOG or KEY types.</p>
     * @return the time interval in milliseconds
     */
    public long getCloseUnusedBlocksAfterMillis() {
        return closeUnusedBlocksAfterMillis;
    }

    /**
     * Gets the time interval in milliseconds after each the unused block sequences will
     * be removed from the memory.
     *
     * <p>Sequences combine several blocks in one continues data block, which is used
     * in the ro-store controlling structures such as catalogs or key blocks.</p>
     *
     * @return the time interval in milliseconds
     */
    public long getCloseUnusedSequencesAfterMillis() {
        return closeUnusedSequencesAfterMillis;
    }

    /**
     * The mapper properties that are calculated based on the storage properties
     *
     * @return the mapper properties
     */
    public MapperProperties getMapperProperties() {
        return mapperProperties;
    }
}
