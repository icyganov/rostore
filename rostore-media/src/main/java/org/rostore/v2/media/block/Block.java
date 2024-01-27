package org.rostore.v2.media.block;

import org.rostore.entity.RoStoreException;
import org.rostore.mapper.MapperProperties;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.block.container.BlockContainer;
import org.rostore.v2.media.block.container.Status;

import java.nio.ByteBuffer;

/**
 * An object to access and modify the data in the
 * boundaries of one block. The data can be accessed by primitives like byte, int, and so further.
 *
 * <p>An interface of the block is based upon {@link ByteBuffer}, but adds some
 * utility functions.</p>
 *
 * <p>This interface has a current position, when the write or read operation is
 * executed - this position is incremented based on the length of the data block.</p>
 * <p>Position can explicitly be modified.</p>
 *
 * <p>The block is all the time associated with one {@link BlockContainer}.</p>
 * <p>Containers should only be used in one process, so that the block instances
 * are also meant to be used withing this one user process.</p>
 * <p>That's why both {@link Block} and {@link BlockContainer} are inherently thread unsafe.</p>
 *
 * <p>When container closes - all the blocks are get closed.</p>
 */
public class Block implements Closeable {
    private final ByteBuffer content;

    private final MappedPhysicalBlock mappedPhysicalBlock;

    private final BlockContainer blockContainer;

    //private StackTraceElement[] elems;

    protected Block(final ByteBuffer byteBuffer,
                    final MappedPhysicalBlock mappedPhysicalBlock,
                    final BlockContainer blockContainer) {
        this.content = byteBuffer;
        this.mappedPhysicalBlock = mappedPhysicalBlock;
        this.blockContainer = blockContainer;
        //this.elems = new Exception().getStackTrace();
    }

    protected void markDirty() {
        mappedPhysicalBlock.setDirty();
    }

    /**
     * Moves current position by the size of the 8 bytes back.
     */
    public void backLong() {
        content.position(content.position()-8);
    }

    /**
     * Moves current position by the size of the 4 bytes back.
     */
    public void backInt() {
        content.position(content.position()-4);
    }

    /**
     * Moves current position by the provided number of bytes back.
     */
    public void back(int bytesPerRecord) {
        content.position(content.position()-bytesPerRecord);
    }

    /**
     * Moves current position by the size of the block index length back (see {@link MapperProperties#getBytesPerBlockIndex()}).
     */
    public void backBlockIndex() {
        content.position(content.position()-blockContainer.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
    }

    /**
     * Provides the index of the current block in the rostore media.
     *
     * @return the block index
     */
    public long getAbsoluteIndex() {
        return mappedPhysicalBlock.getIndex();
    }

    /**
     * Sets current position in the block
     *
     * @param position the position withing the block
     */
    public void position(int position) {
        content.position(position);
    }

    /**
     * Moves current position by the provided number of bytes forward.
     */
    public void skip(int bytes) {
        content.position(content.position() + bytes);
    }

    /**
     * Provides current position in the block
     *
     * @return the position form the start of the block
     */
    public int position() {
        return content.position();
    }

    /**
     * The total size of the block
     *
     * @return size in bytes
     */
    public int length() {
        return content.limit();
    }

    /**
     * The size left in the block starting from current position
     *
     * @return the size in bytes
     */
    public int left() {
        return content.limit() - content.position();
    }

    /**
     * Writes a byte in the current position and increment current position by 1
     * @param b the byte to write
     */
    public void putByte(byte b) {
        markDirty();
        content.put(b);
    }

    /**
     * Writes a long in the current position and increment current position by 8
     * @param l the long to write
     */
    public void putLong(long l) {
        markDirty();
        content.putLong(l);
    }

    /**
     * Writes an int in the current position and increment current position by 4
     * @param i the int to write
     */
    public void putInt(int i) {
        markDirty();
        content.putInt(i);
    }

    /**
     * This function moves the data that starts at position+bytes to position
     * the position is set back to the current position
     *
     * @param windowSize how many bytes to shift back
     * @param tailSize number of bytes to copy
     */
    public void collapseWindow(int windowSize, int tailSize) {
        if (tailSize!=0) {
            markDirty();
            int startAt = content.position();
            int stopAt = startAt + tailSize;
            for (int i = startAt; i < stopAt; i++) {
                content.put(i, content.get(i + windowSize));
            }
        }
    }

    /**
     * To shift all the data in the block that starts at current position
     * and move it to the location shifted at the windowSize to the front
     * (insert operation)
     *
     * @param windowSize the size of the window to clear
     * @param tailSize number of bytes to move
     */
    public void insertWindows(int windowSize, int tailSize) {
        if (tailSize != 0) {
            markDirty();
            int startAt = content.position() + windowSize;
            int stopAt = startAt + tailSize;
            for (int i = stopAt-1; i >= startAt; i--) {
                content.put(i, content.get(i - windowSize));
            }
        }
    }

    /**
     * Writes a block index to the block at current position, and increment position with the size of block index.
     * <p>See {@link MapperProperties#getBytesPerBlockIndex()}</p>
     *
     * @param blockIndex the block index
     */
    public void writeBlockIndex(long blockIndex) {
        putLong(blockIndex, blockContainer.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
    }

    /**
     * Writes a provided number of bytes from the provided long in the current position and increment current position by the
     * provided number of bytes.
     *
     * @param l the long to write
     * @param length number of bytes to write
     */
    public void putLong(long l, int length) {
        if (length != 0) {
            markDirty();
            for (int i = length - 1; i >= 0; i--) {
                long shifted = l >> (i * 8);
                byte b = (byte) (shifted & 0xff);
                content.put(b);
            }
        }
    }

    /**
     * Writes a subset of bytes from byte array
     *
     * @see ByteBuffer#put(byte[], int, int)
     */
    public void put(byte[] data, int offset, int length) {
        if (length!=0) {
            markDirty();
            content.put(data, offset, length);
        }
    }

    /**
     * Puts a given number of bytes from the given block to this one
     *
     * @param sourceBlock the block to copy from
     * @param length number of bytes
     */
    public void put(final Block sourceBlock, final int length) {
        if (length!=0) {
            markDirty();
            int oldLimit = sourceBlock.content.limit();
            int newLimit = sourceBlock.content.position() + length;
            if (newLimit > oldLimit) {
                throw new RoStoreException("Can't move beyond the block boundaries (" + newLimit + "(" + sourceBlock.content.position() + " + " + length +")>" + oldLimit + ")");
            }
            sourceBlock.content.limit(newLimit);
            try {
                content.put(sourceBlock.content);
            } finally {
                sourceBlock.content.limit(oldLimit);
            }
        }
    }

    /**
     * Compares the given number of bytes from starting from the given offset in the byte array with the
     * block's respective data.
     *
     * @param data the byte array to compare with
     * @param offset the offset in the given array to start with
     * @param length the number of bytes to compare
     * @return 0 if all the bytes are equal, negative if the bytes is found in the data that is lower than the respective one in the block, positive - opposite
     */
    public int compare(final byte[] data, final int offset, final int length) {
        int startPosition = content.position();
        for(int i=0; i<length; i++) {
            int res = data[i+offset] - content.get(i+startPosition);
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }

    /**
     * Loads the length number of bytes from the current block's position to the offset in the data array
     * @param data the array to copy to
     * @param offset where the data should start in the data array
     * @param length the number of bytes to copy
     */
    public void get(byte[] data, int offset, int length) {
        if (length!=0) {
            content.get(data, offset, length);
        }
    }

    /**
     * Reads the given number of bytes from the current position and increment the current position by this number of bytes.
     *
     * @param length the number of bytes to read
     * @return the long where the data is put to
     */
    public long getLong(int length) {
        long l = 0;
        for(int i=0; i<length; i++) {
            int b = content.get();
            b &= 0xff;
            l <<= 8;
            l |= b;
        }
        return l;
    }

    /**
     * Reads a block index from the block at current position, and increment position with the size of block index.
     * <p>See {@link MapperProperties#getBytesPerBlockIndex()}</p>
     *
     * @return the block index
     */
    public long readBlockIndex() {
        return getLong(blockContainer.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
    }

    /**
     * Reads a byte from the current position and increments the current position.
     * @return the byte
     */
    public byte getByte() {
        return content.get();
    }

    /**
     * Reads a long at the current position and increment the current position by 8
     *
     * @return the long that has been read
     */
    public long getLong() {
        return content.getLong();
    }

    /**
     * Reads an int at the current position and increment the current position by 4
     *
     * @return the int that has been read
     */
    public int getInt() {
        return content.getInt();
    }

    /**
     * Sets a position to 0 and sets the content to 0
     */
    public void clean() {
        markDirty();
        content.position(0);
        while(content.position()<content.limit()) {
            content.put((byte)0);
        }
        content.position(0);
    }

    public String toString() {
        return "Block: index=" + getAbsoluteIndex() + " pos=" + position() + " " + (mappedPhysicalBlock.isDirty() ? "dirty":"");
    }

    @Override
    public void close() {
        blockContainer.evict(this);
    }

    @Override
    public Status getStatus() {
        return blockContainer.hasBlock(getAbsoluteIndex()) ? Status.OPENED : Status.CLOSED;
    }

}
