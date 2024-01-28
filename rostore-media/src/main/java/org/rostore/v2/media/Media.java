package org.rostore.v2.media;

import org.rostore.entity.BlockAllocation;
import org.rostore.entity.RoStoreException;
import org.rostore.entity.media.MediaPropertiesBuilder;
import org.rostore.v2.data.DataReader;
import org.rostore.v2.data.DataWriter;
import org.rostore.mapper.BinaryMapper;
import org.rostore.v2.media.block.MappedPhysicalBlocks;
import org.rostore.v2.media.block.allocator.*;
import org.rostore.v2.media.block.container.BlockContainer;
import org.rostore.v2.seq.BlockIndexSequences;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A major class representing the ro-store storage.
 * <p>It is a basic media, which represents a simple basic building block
 * of the media, which can be extended.</p>
 */
public class Media extends RootClosableImpl {

    private static final Logger logger = Logger.getLogger(Media.class.getName());
    public static final byte MAGIC = 0x77;
    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final MappedPhysicalBlocks mappedPhysicalBlocks;
    private final BlockIndexSequences blockIndexSequences;
    private Map<Integer, BlockContainer> blockContainers = new HashMap<>();
    /** Lists all the block sequences: first block -> sequence */
    private BlockAllocator rootBlockAllocator;
    private int blockContainerCount = 0;
    private MediaProperties mediaProperties;

    private BlockAllocatorListeners blockAllocatorListeners = new BlockAllocatorListeners();

    public BlockAllocatorListeners getBlockAllocatorListeners() {
        return blockAllocatorListeners;
    }

    public MediaProperties getMediaProperties() {
        return mediaProperties;
    }

    public BlockAllocator getBlockAllocator() {
        return rootBlockAllocator;
    }

    /**
     * Provides information about the objects this {@link Media} holds.
     *
     * @return info about the memory consumption
     */
    public MemoryConsumption getMemoryConsumption() {
        return new MemoryConsumption(mappedPhysicalBlocks.size(), blockIndexSequences.size(), blockContainers.size());
    }

    /**
     * Provides the set of currently opened {@link org.rostore.v2.seq.BlockIndexSequence}
     *
     * @return media's currently active block sequences
     */
    public BlockIndexSequences getBlockIndexSequences() {
        return blockIndexSequences;
    }

    /**
     * Provides the set of currently mapped physical blocks
     *
     * @return media's currently active blocks
     */
    public MappedPhysicalBlocks getMappedPhysicalBlocks() {
        return mappedPhysicalBlocks;
    }

    /**
     * Closes the instance of media.
     *
     * <p>This function will not regard any open {@link BlockContainer}, the caller should finish
     * all the transactions before.</p>
     */
    @Override
    public void close() {
        super.close();
        rootBlockAllocator.close();
        try {
            randomAccessFile.close();
        }catch (IOException ioException) {
            throw new RoStoreException("Can't close " + file);
        }
    }

    /**
     * Provides information about block allocation within the media
     *
     * <p>As a source the {@link RootBlockAllocator} is used.</p>
     *
     * @return the block allocation
     */
    public BlockAllocation getBlockAllocation() {
        return rootBlockAllocator.getBlockAllocatorInternal().getBlockAllocation();
    }

    /**
     * Create a new media
     *
     * <p>A {@param headerStream} is used to extend current media by any functionality that is build on top of it.</p>
     * <p>The media object is fully constructed when the consumer is executed, so any construction can happen on
     * its basis. The consumer should write the necessary data to the {@link DataWriter} so that the state of
     * the construction can be persisted.</p>
     *
     * @param file a file where the data should be persisted
     * @param mediaProperties the properties of the media
     * @param headerStream consumer that receives both the created media object and a {@link DataWriter} that can be used to write
     *                     additional header information, which allows to extend the media header.
     */
    protected Media(final File file, final MediaProperties mediaProperties, final BiConsumer<Media, DataWriter> headerStream) {
        logger.log(Level.INFO, "Create a new media @" + file);
        this.mediaProperties = mediaProperties;
        mappedPhysicalBlocks = new MappedPhysicalBlocks(this);
        blockIndexSequences = new BlockIndexSequences(this);
        this.file = file;
        if (mediaProperties.getMapperProperties().getBytesPerBlockIndex() > 8) {
            throw new RoStoreException("The maximum number of bytes for block index is 8, provided " + mediaProperties.getMapperProperties().getBytesPerBlockIndex());
        }
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.setLength(mediaProperties.getMaxTotalSize());
            rootBlockAllocator = RootBlockAllocator.create(this);
            try (final DataWriter dataWriter = DataWriter.open(rootBlockAllocator, 0)) {
                MediaHeader mediaHeader = new MediaHeader();
                mediaHeader.setMagic(MAGIC);
                mediaHeader.setBlockSize(mediaProperties.getBlockSize());
                mediaHeader.setMaxTotalSize(mediaProperties.getMaxTotalSize());
                mediaHeader.setCloseUnusedBlocksAfterMillis(mediaProperties.getCloseUnusedBlocksAfterMillis());
                mediaHeader.setCloseUnusedSequencesAfterMillis(mediaProperties.getCloseUnusedSequencesAfterMillis());
                dataWriter.writeObject(mediaHeader);
                if (headerStream != null) {
                    headerStream.accept(this, dataWriter);
                }
            }
        } catch (final IOException e) {
            throw new RoStoreException("Can't open " + file,  e);
        }
    }

    public void closeExpired() {
        blockIndexSequences.closeExpired();
        mappedPhysicalBlocks.closeExpired();
    }

    protected Media(final File file, BiConsumer<Media, DataReader> headerStream) {
        logger.log(Level.INFO, "Opening media @" + file);
        this.file = file;
        mappedPhysicalBlocks = new MappedPhysicalBlocks(this);
        blockIndexSequences = new BlockIndexSequences(this);
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            byte[] head = new byte[13];
            randomAccessFile.read(head);
            MediaHeader mediaHeaderShort = BinaryMapper.deserialize(null, MediaHeader.class, new ByteArrayInputStream(head), 2);
            MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
            mediaPropertiesBuilder.setBlockSize(mediaHeaderShort.getBlockSize());
            mediaPropertiesBuilder.setMaxTotalSize(mediaHeaderShort.getMaxTotalSize());
            mediaProperties = MediaProperties.from(mediaPropertiesBuilder);
            rootBlockAllocator = RootBlockAllocator.load(this);

            try (final DataReader dataReader = DataReader.open(rootBlockAllocator, 0)) {
                final MediaHeader mediaHeader = dataReader.readObject(MediaHeader.class);
                if (MAGIC != mediaHeader.getMagic()) {
                    throw new RoStoreException("File " + file + " has a wrong structure");
                }
                mediaPropertiesBuilder.setCloseUnusedBlocksAfterMillis(mediaHeader.getCloseUnusedBlocksAfterMillis());
                mediaPropertiesBuilder.setCloseUnusedSequencesAfterMillis(mediaHeader.getCloseUnusedSequencesAfterMillis());
                mediaProperties = MediaProperties.from(mediaPropertiesBuilder);
                if (headerStream != null) {
                    headerStream.accept(this, dataReader);
                }
            }

        } catch (final IOException e) {
            throw new RoStoreException("Can't open " + file,  e);
        }
    }



    /**
     * Creates a blank media
     * @param file
     * @param mediaProperties
     * @return
     */
    public static Media create(final File file, final MediaProperties mediaProperties) {
        return create(file,
                mediaProperties,
                (BiConsumer<Media, DataWriter>) null);
    }

    /**
     * Creates a new media and store a header class to the header of the media
     * @param file
     * @param mediaProperties
     * @param headerFactory creates a header object that will be stored to the header
     * @return the created and stored header
     * @param <T> a type of the header object, must be serializable by the {@link BinaryMapper}
     */
    public static <T> Media create(final File file, final MediaProperties mediaProperties, final Function<Media, T> headerFactory) {
        return create(file,
                mediaProperties,
                (m,dataWriter) -> dataWriter.writeObject(headerFactory.apply(m)));
    }

    /**
     * Creates a new media and store with an option to store to headers stream of the media
     * @param file
     * @param mediaProperties
     * @param headerStream a consumer that can write any binary data to the header
     * @return the created media
     */
    public static Media create(final File file, final MediaProperties mediaProperties, final BiConsumer<Media, DataWriter> headerStream) {
        final Media media = new Media(file, mediaProperties, headerStream);
        return media;
    }

    /**
     * Load a black data
     * @param file
     * @return
     */
    public static Media open(final File file) {
        return open(file, null);
    }

    /**
     * Opens a media, reads the header and provides it for further initialization
     * @param file
     * @param headerClass a class of the custom header, must be serializable by {@link BinaryMapper}
     * @param header
     * @return the media object
     * @param <T>
     */
    public static <T> Media open(final File file, final Class<T> headerClass, final BiConsumer<Media, T> header) {
        return open(file, (m, dataReader) -> header.accept(m, dataReader.readObject(headerClass)));
    }

    /**
     * Opens a media, allows to read the header from a binary stream
     * @param file
     * @param headerStream a stream that allows to read from the header
     * @return opened media
     */
    public static Media open(final File file, final BiConsumer<Media, DataReader> headerStream) {
        final Media media = new Media(file, headerStream);
        return media;
    }

    public MappedByteBuffer map(final long index) {
        long startOffset = index * mediaProperties.getBlockSize();
        try {
            return randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, startOffset, mediaProperties.getBlockSize());
        } catch (final IOException ioException) {
            throw new RoStoreException("Can't map " + file + " from " + startOffset + "(index=" + index + "), length=" + mediaProperties.getBlockSize(), ioException);
        }
    }

    /**
     * The Block container must be closed after wards
     * @return
     */
    public synchronized BlockContainer newBlockContainer() {
        final int blockContainerId = blockContainerCount++;
        final BlockContainer blockContainer = new BlockContainer(this, blockContainerId);
        blockContainers.put(blockContainerId, blockContainer);
        return blockContainer;
    }

    public synchronized BlockContainer getBlockContainer(final int blockContainerId) {
        return blockContainers.get(blockContainerId);
    }

    public synchronized void freeBlockContainer(final int blockContainerId) {
        blockContainers.remove(blockContainerId);
    }

    public synchronized BlockAllocator createSecondaryBlockAllocator(final String allocatorName, final long upperBlockNumberLimit) {
        return SecondaryBlockAllocator.create(allocatorName, rootBlockAllocator, upperBlockNumberLimit);
    }

    public synchronized BlockAllocator loadSecondaryBlockAllocator(final String allocatorName, final long startIndex, final long upperBlockNumberLimit) {
        return SecondaryBlockAllocator.load(allocatorName, rootBlockAllocator, startIndex, upperBlockNumberLimit);
    }

    public synchronized void removeSecondaryBlockAllocator(final BlockAllocator blockAllocator) {
        blockAllocator.remove();
    }

    public void dump() {
        rootBlockAllocator.getBlockAllocatorInternal().dump();
    }

}
