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
    // (data)
    // next block

    // last block
    // (data)
    // length

    // first block == last block
    // --------------------------
    // (data)
    // length
    // next block == itself

    public static long safeWriter(final BlockAllocator blockAllocator,
                                  final Consumer<DataWriter> dataWriterConsumer) {
        return safeWriter(blockAllocator, Utils.ID_UNDEFINED, dataWriterConsumer);
    }

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

    public static DataWriter open(final BlockAllocator blockAllocator, final long startIndex) {
        return new DataWriter(BlockProviderImpl.internal(blockAllocator), startIndex);
    }

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

    public <T> void writeObject(final T object) {
        BinaryMapper.serialize(internalBlockProvider.getMedia().getMediaProperties().getMapperProperties(), object, this);
    }

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

    // this should be called if operation has been aborted and must be set back
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

    @Override
    public void commit() {
        internalBlockProvider.getBlockContainer().commit();
    }
}
