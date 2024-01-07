package org.rostore.v2.data;

import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.mapper.BinaryMapper;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.Media;
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

/**
 * This is a counterpart of {@link DataWriter}, that allows to read the data from the storage that
 * has been previously written by the writer.
 * Is not thread-safe
 */
public class DataReader extends InputStream implements Committable {

    final BlockProvider internalBlockProvider;

    private long root;
    private Block current;

    private long length = 0;
    private long position = 0;

    private long lastIndex;

    // 1st block
    // --------------------------
    // (data)
    // last block
    // next block

    // nth block
    // --------------------------
    // (data)
    // next block

    // last block
    // --------------------------
    // (data)
    // length

    // first block == last block
    // --------------------------
    // (data)
    // length
    // next block == itself

    /**
     * Opens the data reader to start reading from the given block.
     * <p>The data reader should not be used to free the blocks {@link #free()}, as it will
     * use the root allocator for it.</p>
     *
     * @param media the media to be used
     * @param startIndex the first block to start reading from
     * @return the data reader object
     */
    public static DataReader open(final Media media, final long startIndex) {
        return new DataReader(BlockProviderImpl.internal(media), startIndex);
    }

    /**
     * Opens the data reader to start reading from the given block.
     * <p>This is a more specific operation comparing to {@link #open(Media, long)}, as
     * it specifies a specific allocator, and not the root's one.</p>
     * <p>The {@link #free()} can be executed.</p>
     *
     * @param allocator allocator to be used
     * @param startIndex the first block to start reading from
     * @return the data reader object
     */
    public static DataReader open(final BlockAllocator allocator, final long startIndex) {
        return new DataReader(BlockProviderImpl.internal(allocator), startIndex);
    }

    public static void safeReader(final Media media, final long startIndex, final Consumer<DataReader> dataReaderConsumer) {
        try (final DataReader dr = DataReader.open(media, startIndex)) {
            dataReaderConsumer.accept(dr);
        } catch (final Exception e) {
            throw new DataTransferException(e);
        }
    }

    public static <T> T readObject(final Media media, final long startIndex, final Class<T> clazz) {
        try (final DataReader dr = DataReader.open(media, startIndex)) {
            return dr.readObject(clazz);
        } catch (final Exception e) {
            throw new DataTransferException(e);
        }
    }

    /**
     * Reads the data from the sequence starting at start index and writes it to the output stream.
     * @param media the media to read from
     * @param startIndex the first block index
     * @param outputStream the output stream to write to
     * @param <T> the subtype of the output stream
     */
    public static <T extends OutputStream> void toOutputStream(final Media media, final long startIndex, final T outputStream) {
        try (final DataReader dr = DataReader.open(media, startIndex)) {
            dr.transferTo(outputStream);
        } catch (final IOException e) {
            throw new DataTransferException(e);
        }
    }

    /**
     * @param internalBlockProvider used to allocate the blocks
     */
    private DataReader(final BlockProvider internalBlockProvider, final long startIndex) {
        this.internalBlockProvider = internalBlockProvider;
        this.root = startIndex;
        this.current = internalBlockProvider.getBlockContainer().getBlock(root, BlockType.DATA);
        current.position(internalBlockProvider.getMedia().getMediaProperties().getBlockSize() - internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        long nextIndex = current.readBlockIndex();
        if (nextIndex == root) {
            lastIndex = root;
            current.position(internalBlockProvider.getMedia().getMediaProperties().getBlockSize() - internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() - 1);
            int lengthLength = current.getByte();
            current.back(lengthLength+1);
            length = current.getLong(lengthLength);
        } else {
            current.position(internalBlockProvider.getMedia().getMediaProperties().getBlockSize() - internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex()*2);
            lastIndex = current.readBlockIndex();
            Block lastBlock = internalBlockProvider.getBlockContainer().getBlock(lastIndex, BlockType.DATA);
            lastBlock.position(internalBlockProvider.getMedia().getMediaProperties().getBlockSize() - 1);
            int lengthLength = lastBlock.getByte();
            lastBlock.back(lengthLength+1);
            length = lastBlock.getLong(lengthLength);
        }
        current.position(0);
    }

    /**
     * Reads the java object from the data reader.
     * <p>The function uses {@link BinaryMapper} to deserialize the object.</p>
     *
     * @param clazz the class of the java object
     * @return the java object
     * @param <T> the type of the object
     */
    public <T> T readObject(final Class<T> clazz) {
        return BinaryMapper.deserialize(internalBlockProvider.getMedia().getMediaProperties().getMapperProperties(), clazz, this);
    }

    public long length() {
        return length;
    }

    public long position() {
        return position;
    }

    public int read() {
        if (position >= length) {
            return -1;
        }
        if (root == lastIndex) {
            position++;
            return current.getByte() & 0xff;
        }

        if (current.getAbsoluteIndex() == lastIndex) {
            position++;
            return current.getByte() & 0xff;
        }
        int capacity = getRegularCapacity();
        if (root == current.getAbsoluteIndex()) {
            // this is a first block
            capacity -= internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
        }
        if (capacity <= 0) {
            moveToNextBlock();
        }
        position++;
        return current.getByte() & 0xff;
    }

    /**
     * Operation is to free the blocks that has previously been allocated by the {@link DataWriter}.
     * <p>Note, that the {@link BlockAllocator} should be correctly provided {@link #open(BlockAllocator, long)}.</p>
     * <p>If the {@link #open(Media, long)} is used, the root allocator will be used.</p>
     */
    public void free() {
        iterateIndices(catalogBlockIndices -> {
            internalBlockProvider.getBlockAllocator().free(catalogBlockIndices);
        });
    }

    /**
     * Executed block by block with its content.
     * <p>For the very long data sequences, it might be called several times.</p>
     */
    public void iterateIndices(final Consumer<CatalogBlockIndices> consumer) {
        current = internalBlockProvider.getBlockContainer().getBlock(root, BlockType.DATA);
        final CatalogBlockIndices indices = new CatalogBlockIndices();
        do {
            indices.add(current.getAbsoluteIndex(), current.getAbsoluteIndex());
            if (current.getAbsoluteIndex() == lastIndex) {
                current.close();
                consumer.accept(indices);
                return ;
            }
            current.position(internalBlockProvider.getMedia().getMediaProperties().getBlockSize()-
                    internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
            long nextBlock = current.readBlockIndex();
            current.close();
            current = internalBlockProvider.getBlockContainer().getBlock(nextBlock, BlockType.DATA);
            if (indices.getGroupNumber() >= Properties.DELETE_GROUPS) {
                consumer.accept(indices);
                indices.clear();
            }
        } while (true);
    }

    public boolean hasMore() {
        return position < length;
    }

    private int getRegularCapacity() {
        int capacity = internalBlockProvider.getMedia().getMediaProperties().getBlockSize();
        capacity -= current.position();
        capacity -= internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
        return capacity;
    }

    private void moveToNextBlock() {
        current.position(internalBlockProvider.getMedia().getMediaProperties().getBlockSize() - internalBlockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        final long nextIndex = current.readBlockIndex();
        final Block next = internalBlockProvider.getBlockContainer().getBlock(nextIndex, BlockType.DATA);
        next.position(0);
        // this evicts the current block
        current.close();
        current = next;
    }

    @Override
    public void close() {
        try {
            super.close();
            this.internalBlockProvider.getBlockContainer().close();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Status getStatus() {
        return internalBlockProvider.getBlockContainer().getStatus();
    }

    @Override
    public void commit() {
        internalBlockProvider.getBlockContainer().commit();
    }
}
