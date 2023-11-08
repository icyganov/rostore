package org.rostore.v2.media.block;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.block.container.BlockContainer;
import org.rostore.v2.media.block.container.Status;

import java.nio.ByteBuffer;

public class Block implements Committable {
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

    protected MappedPhysicalBlock getAllocatedBlock() {
        return mappedPhysicalBlock;
    }

    protected void markDirty() {
        mappedPhysicalBlock.setDirty();
    }

    public void backLong() {
        content.position(content.position()-8);
    }

    public void backInt() {
        content.position(content.position()-4);
    }

    public void back(int bytesPerRecord) {
        content.position(content.position()-bytesPerRecord);
    }

    public void backBlockIndex() {
        content.position(content.position()-blockContainer.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
    }

    public long getAbsoluteIndex() {
        return mappedPhysicalBlock.getIndex();
    }

    public void position(int position) {
        content.position(position);
    }

    public void skip(int bytes) {
        content.position(content.position() + bytes);
    }

    public int position() {
        return content.position();
    }

    public int length() {
        return content.limit();
    }

    public int left() {
        return content.limit() - content.position();
    }

    public void putByte(byte b) {
        markDirty();
        content.put(b);
    }

    public void putLong(long l) {
        markDirty();
        content.putLong(l);
    }

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

    public void writeBlockIndex(long l) {
        putLong(l, blockContainer.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
    }

    public void putLong(long l, int bytesPerRecord) {
        if (bytesPerRecord != 0) {
            markDirty();
            for (int i = bytesPerRecord - 1; i >= 0; i--) {
                long shifted = l >> (i * 8);
                byte b = (byte) (shifted & 0xff);
                content.put(b);
            }
        }
    }

    public void put(byte[] data, int offset, int length) {
        if (length!=0) {
            markDirty();
            content.put(data, offset, length);
        }
    }

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

    public int compare(byte[] data, int offset, int length) {
        int startPosition = content.position();
        for(int i=0; i<length; i++) {
            int res = data[i+offset] - content.get(i+startPosition);
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }

    public void get(byte[] data, int offset, int length) {
        if (length!=0) {
            content.get(data, offset, length);
        }
    }

    public long getLong(int bytesPerRecord) {
        long l = 0;
        for(int i=0; i<bytesPerRecord; i++) {
            int b = content.get();
            b &= 0xff;
            l <<= 8;
            l |= b;
        }
        return l;
    }

    public long readBlockIndex() {
        return getLong(blockContainer.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
    }

    public byte getByte() {
        return content.get();
    }

    public long getLong() {
        return content.getLong();
    }

    public int getInt() {
        return content.getInt();
    }

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

    @Override
    public void commit() {
        mappedPhysicalBlock.flush();
    }
}
