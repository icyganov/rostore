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

    /**
     * Provides the properties that has been used for the creation of the media
     *
     * @return the media properties
     */
    public MediaProperties getMediaProperties() {
        return mediaProperties;
    }

    /**
     * Provides the root block allocator.
     *
     * @return the root block allocator
     */
    public BlockAllocator getRootBlockAllocator() {
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

    /**
     * This function can be called periodically to close
     * the unused blocks and sequences.
     * <p>This is a proactive operation. The clean-up is also executed on every operation,
     * but to ensure that the clean-up is also executed in the time when media is idling
     * this operation can be used.</p>
     */
    public void closeExpired() {
        blockIndexSequences.closeExpired();
        mappedPhysicalBlocks.closeExpired();
    }

    /**
     * Load an existing media
     *
     * <p>A {@param headerStream} is used to read the header information that has previously been stored
     * at the creation time to the header of the media in the call to {@link #Media(File, MediaProperties, BiConsumer)}.
     * This allows to restore the entities created in the media.</p>
     *
     * @param file a file where the data is stored
     * @param headerStream consumer that receives both the created media object and a {@link DataReader} that can be used to read
     *                     additional header information, which allows to extend the media basic functionality.
     */
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
     *
     * @param file the file where the media should be persisted
     * @param mediaProperties the properties of the media
     * @return the constructed media object
     */
    public static Media create(final File file, final MediaProperties mediaProperties) {
        return create(file,
                mediaProperties,
                (BiConsumer<Media, DataWriter>) null);
    }

    /**
     * Creates a new media and store a header class to the header of the media
     * @param file the file location where the media data should be persisted
     * @param mediaProperties the properties of the media
     * @param headerFactory creates a header object that will be stored as additional information to the media's header
     * @return the created media object
     * @param <T> a type of the header object, must be serializable by the {@link BinaryMapper}
     */
    public static <T> Media create(final File file, final MediaProperties mediaProperties, final Function<Media, T> headerFactory) {
        return create(file,
                mediaProperties,
                (m,dataWriter) -> dataWriter.writeObject(headerFactory.apply(m)));
    }

    /**
     * Creates a new media and store with an option to store to headers stream of the media.
     *
     * @param file the file where the storage data should be persisted
     * @param mediaProperties the properties of the storage
     * @param headerStream a consumer that can write any binary data to the header
     * @return the created media
     */
    public static Media create(final File file, final MediaProperties mediaProperties, final BiConsumer<Media, DataWriter> headerStream) {
        final Media media = new Media(file, mediaProperties, headerStream);
        return media;
    }

    /**
     * Opens the media object from the existing file
     *
     * @param file the file where the storage is located
     * @return the media object
     */
    public static Media open(final File file) {
        return open(file, null);
    }

    /**
     * Opens a media, reads the header and provides it for further initialization
     *
     * @param file the file where the storage data is located
     * @param headerClass a class of the custom header, must be serializable by {@link BinaryMapper}
     * @param header bi consumer that provides both fully initialized media object and the deserialized header object
     * @return the media object
     * @param <T> the type of the header class
     */
    public static <T> Media open(final File file, final Class<T> headerClass, final BiConsumer<Media, T> header) {
        return open(file, (m, dataReader) -> header.accept(m, dataReader.readObject(headerClass)));
    }

    /**
     * Opens a media, allows to read the header from a binary stream
     *
     * @param file the file where the storage data is located
     * @param headerStream consumer that receives both the fully initialized media object and a {@link DataReader} that can be used to
     *                     read additional header information, to initialize further entities
     * @return opened media
     */
    public static Media open(final File file, final BiConsumer<Media, DataReader> headerStream) {
        final Media media = new Media(file, headerStream);
        return media;
    }

    /**
     * A low-level operation of mapping of the block referenced by its index to
     * the mapped byte buffer.
     * <p>This operation should not be used directly by the clients of media,
     * instead {@link org.rostore.v2.media.block.InternalBlockProvider} should
     * be used to get access to the block's data.</p>
     *
     * @param blockIndex the index of the block
     *
     * @return the mapped memory block
     */
    public MappedByteBuffer map(final long blockIndex) {
        long startOffset = blockIndex * mediaProperties.getBlockSize();
        try {
            return randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, startOffset, mediaProperties.getBlockSize());
        } catch (final IOException ioException) {
            throw new RoStoreException("Can't map " + file + " from " + startOffset + "(index=" + blockIndex + "), length=" + mediaProperties.getBlockSize(), ioException);
        }
    }

    /**
     * Creates a new Block container, which represents a transactional boundary in ro-store,
     * which must be closed after usage.
     *
     * @return the block container
     */
    public synchronized BlockContainer newBlockContainer() {
        final int blockContainerId = blockContainerCount++;
        final BlockContainer blockContainer = new BlockContainer(this, blockContainerId);
        blockContainers.put(blockContainerId, blockContainer);
        return blockContainer;
    }

    /**
     * Provides a block container by its id
     * <p>Should not be used directly.</p>
     *
     * @param blockContainerId the id of the block container
     * @return the block container
     */
    public synchronized BlockContainer getBlockContainer(final int blockContainerId) {
        return blockContainers.get(blockContainerId);
    }

    /**
     * Frees a block container
     * <p>Should not be used directly.</p>
     * @param blockContainerId the id of the block container
     */
    public synchronized void freeBlockContainer(final int blockContainerId) {
        blockContainers.remove(blockContainerId);
    }

    /**
     * Creates {@link SecondaryBlockAllocator} based on the internal root allocator.
     * See {@link SecondaryBlockAllocator#create(String, BlockAllocator, long)}
     *
     * @param allocatorName the name of the allocator
     * @param upperBlockNumberLimit the maximum number of blocks
     * @return the newly created secondary allocator
     */
    public synchronized BlockAllocator createSecondaryBlockAllocator(final String allocatorName, final long upperBlockNumberLimit) {
        return SecondaryBlockAllocator.create(allocatorName, rootBlockAllocator, upperBlockNumberLimit);
    }

    /**
     * Loads {@link SecondaryBlockAllocator} based on the internal root allocator.
     * See {@link SecondaryBlockAllocator#load(String, BlockAllocator, long, long)}
     *
     * <p>Media do not hold instances of the secondary allocators, so the client
     * should hold them and prevent several instances of these allocator to be loaded.</p>
     *
     * @param allocatorName the name of the allocator
     * @param startIndex the first block of the allocator
     * @param upperBlockNumberLimit the maximum number of blocks
     * @return the loaded secondary allocator
     */
    public synchronized BlockAllocator loadSecondaryBlockAllocator(final String allocatorName, final long startIndex, final long upperBlockNumberLimit) {
        return SecondaryBlockAllocator.load(allocatorName, rootBlockAllocator, startIndex, upperBlockNumberLimit);
    }

    /**
     * Call to this function will free all the blocks allocated in the secondary allocator.
     *
     * @param blockAllocator the allocator to clean up
     */
    public synchronized void removeSecondaryBlockAllocator(final BlockAllocator blockAllocator) {
        blockAllocator.remove();
    }

    public void dump() {
        rootBlockAllocator.getBlockAllocatorInternal().dump();
    }

}
