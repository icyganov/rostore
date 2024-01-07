package org.rostore.v2.data;

import org.rostore.Utils;
import org.rostore.mapper.BinaryMapper;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.BlockProviderImpl;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.block.container.Status;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class to write the data to the storage using the provided block allocator.
 * <p>The data is provided in the form of the {@link InputStream}, byte array or java object.</p>
 * <p>It can allocate all the blocks or start writing to some predefined block and allocate the rest,
 * the later is handy for the headers of the {@link org.rostore.v2.media.Media}, or for storages with the predefined layout.</p>
 * <p>The operations are written for the input streams that have no predefined length,
 * the data is written to the storage as it comes.</p>
 * <p>In case of any error (e.g. if space is not enough to store the data), the allocated
 * data will safely be released and an error will be thrown.</p>
 */
public class DataWriter extends OutputStream implements Committable {

    private final static Logger logger = Logger.getLogger(DataWriter.class.getName());

    final BlockProvider internalBlockProvider;

    private long root;
    private Block current = null;

    private long length;

    // 1st block
    // --------------------------
    // (data)
    // last block
    // next block

    // nth block
    // -------
    // (data)
    // next block

    // last block
    // -----
    // (data)
    // length

    // first block == last block
    // --------------------------
    // (data)
    // length
    // next block == itself

    /**
     * Write the data from the unknown source to the storage in a safe mode.
     * <p>The operation creates a data writer object and executes the write operations as a consumer,
     * if any error during write operation happens, it get reverted and only after it the exception is thrown.</p>
     * <p>There is no need for any additional clean up in the case of error.</p>
     *
     * @param blockAllocator the block allocator be used
     * @param dataWriterConsumer the consumer that receive the data writer object and should provide the data to it
     * @return the first block index
     */
    public static long safeWriter(final BlockAllocator blockAllocator,
                                  final Consumer<DataWriter> dataWriterConsumer) {
        return safeWriter(blockAllocator, Utils.ID_UNDEFINED, dataWriterConsumer);
    }

    /**
     * Writes the serialized java object to the store in the safe manner.
     * <p>It is similar to {@link #safeWriter(BlockAllocator, Consumer)}, only the data is provided in the for of java object.</p>
     * <p>The java object is serialized with {@link BinaryMapper}.</p>
     * @param blockAllocator the block allocator to get the blocks
     * @param object the java object to store
     * @return the index of the first allocated block
     * @param <T> the type of the java object
     */
    public static <T> long writeObject(final BlockAllocator blockAllocator,
                                       final T object) {
        return safeWriter(blockAllocator, Utils.ID_UNDEFINED, (dw) ->
                dw.writeObject(object));
    }

    public void write(byte[] b) {
        try {
            super.write(b);
        } catch (IOException e) {
            throw new DataTransferException(e);
        }
    }

    /**
     * Write the data from the unknown source to the storage in a safe mode.
     * <p>This one is similar to {@link #safeWriter(BlockAllocator, Consumer)}, only the first block index is provided explicitly,
     * all other blocks are allocated by the block allocator.</p>
     * @param blockAllocator the block allocator to be used
     * @param startIndex the index of the first block
     * @param dataWriterConsumer the secured consumer that is writing to this data writer
     * @return the start index
     */
    public static long safeWriter(final BlockAllocator blockAllocator,
                                        final long startIndex,
                                        final Consumer<DataWriter> dataWriterConsumer) {
        final DataWriter dataWriter = open(blockAllocator, startIndex);
        try {
            dataWriterConsumer.accept(dataWriter);
            long id = dataWriter.getStartIndex();
            dataWriter.close();
            return id;
        } catch (final Exception e) {
            // if anything bad happens...
            try {
                dataWriter.unwind();
            } catch(final Exception unwindE) {
                logger.log(Level.SEVERE, "Exception happened in data-writing operation", e);
                throw new DataTransferException("Exception after broken data-writing unwinding", unwindE);
            }
            throw new DataTransferException(e);
        }

    }

    /**
     * Opens the data writer at the specific starting block index
     *
     * @param blockAllocator allocator to be used
     * @param startIndex the block index to start writing to
     * @return the data writer object
     */
    public static DataWriter open(final BlockAllocator blockAllocator, final long startIndex) {
        return new DataWriter(BlockProviderImpl.internal(blockAllocator), startIndex);
    }

    /**
     * Writes the data from the input stream
     * <p>Operation will be reverted if any error happens.</p>
     *
     * @param blockAllocator the block allocator to be used
     * @param inputStream the input stream with the data
     * @return the first block of the data
     * @param <T> the subtype of the input stream
     */
    public static <T extends InputStream> long fromInputStream(final BlockAllocator blockAllocator, final T inputStream) {
        return safeWriter(blockAllocator, (dw) -> {
            try {
                inputStream.transferTo(dw);
            } catch (final IOException e) {
                throw new DataTransferException(e);
            }
        });
    }

    /**
     * This is a special writer, should be used when the first block has already been reserved (for the header for example)
     * @param blockProvider
     * @param startIndex the index of first already reserved block
     */
    private DataWriter(final BlockProvider blockProvider, final long startIndex) {
        this.internalBlockProvider = blockProvider;
        this.root = startIndex;
        if (startIndex != Utils.ID_UNDEFINED) {
            current = internalBlockProvider.getBlockContainer().getBlock(root, BlockType.DATA);
            current.position(0);
        }
        this.length = 0;
    }

    /**
     * Provides the first block of the stored data
     * @return the first block index
     */
    public long getStartIndex() {
        return root;
    }

    private void stop() {
        if (length == 0) {
            return ;
        }

        int lengthLength = Utils.computeBytesForMaxValue(length);
        int capacity = getRegularCapacity();

        if (root == current.getAbsoluteIndex()) {
            if (capacity >= lengthLength + 1) {
                current.position(
                        internalBlockProvider.getMedia().getMediaProperties().getBlockSize() -
                                internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() -
                                lengthLength - 1);
                current.putLong(length, lengthLength);
                current.putByte((byte)lengthLength);
                current.writeBlockIndex(root);
            } else {
                attachNextBlock();
                finalizeLast(lengthLength);
            }
        } else {
            if (capacity < lengthLength) {
                attachNextBlock();
            }
            finalizeLast(lengthLength);
        }
    }

    private void finalizeLast(int lengthLength) {
        current.position(
                internalBlockProvider.getMedia().getMediaProperties().getBlockSize() -
                        lengthLength - 1);
        current.putLong(length, lengthLength);
        current.putByte((byte)lengthLength);
        Block rootBlock = internalBlockProvider.getBlockContainer().getBlock(root, BlockType.DATA);
        rootBlock.position(
                internalBlockProvider.getMedia().getMediaProperties().getBlockSize() -
                        internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex()*2);
        rootBlock.writeBlockIndex(current.getAbsoluteIndex());
    }

    /**
     * Writes a java object to the storage
     * <p>The object is serialized by {@link BinaryMapper}.</p>
     * @param object the object to store
     * @param <T> the type of the object
     */
    public <T> void writeObject(final T object) {
        BinaryMapper.serialize(internalBlockProvider.getMedia().getMediaProperties().getMapperProperties(), object, this);
    }

    /**
     * Writes one byte of the data
     *
     * @param data the {@code byte}.
     */
    public void write(final int data) {
        if (length == 0 && root == Utils.ID_UNDEFINED) {
            current = internalBlockProvider.allocateBlock(BlockType.DATA);
            root = current.getAbsoluteIndex();
            current.position(0);
        }
        int capacity = getRegularCapacity();
        if (root == current.getAbsoluteIndex()) {
            // this is a first block
            capacity -= internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
        }
        if (capacity <= 0) {
            attachNextBlock();
        }
        current.putByte((byte)data);
        length++;
    }

    private int getRegularCapacity() {
        int capacity = internalBlockProvider.getMedia().getMediaProperties().getBlockSize();
        if (current != null) {
            capacity -= current.position();
        }
        capacity -= internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
        return capacity;
    }

    private void attachNextBlock() {
        Block next = internalBlockProvider.allocateBlock(BlockType.DATA);
        next.position(0);
        current.position(internalBlockProvider.getMedia().getMediaProperties().getBlockSize() - internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        current.writeBlockIndex(next.getAbsoluteIndex());
        if (current.getAbsoluteIndex() != root) {
            // if it is NOT the first block => this block can be evicted
            current.close();
        }
        current = next;
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
        stop();
        internalBlockProvider.getBlockContainer().close();
    }

    @Override
    public Status getStatus() {
        return internalBlockProvider.getBlockContainer().getStatus();
    }

    /**
     * This should be called if operation has been aborted and must be set back.
     * It is done automatically if safe mode operations are used, like those started with {@link #safeWriter(BlockAllocator, Consumer) or {@link #safeWriter(BlockAllocator, long, Consumer)}}
     */
    public void unwind() {
        if (current == null) {
            return;
        }
        if (current.getAbsoluteIndex() == root) {
            // this was only one
            internalBlockProvider.freeBlock(root);
            internalBlockProvider.getBlockContainer().close();
        } else {
            long next = root;
            do {
                final Block iterator = internalBlockProvider.getBlockContainer().getBlock(next, BlockType.DATA);
                iterator.position(internalBlockProvider.getMedia().getMediaProperties().getBlockSize() - internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
                final long nextNext = iterator.readBlockIndex();
                internalBlockProvider.freeBlock(next);
                next = nextNext;
            } while (next != current.getAbsoluteIndex());
            internalBlockProvider.freeBlock(next);
            internalBlockProvider.getBlockContainer().close();
        }
    }

    /**
     * Synchronizes the written data with the storage
     */
    @Override
    public void commit() {
        internalBlockProvider.getBlockContainer().commit();
    }
}
