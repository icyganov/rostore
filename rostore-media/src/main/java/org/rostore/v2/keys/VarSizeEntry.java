package org.rostore.v2.keys;

import org.rostore.v2.media.block.Block;

/**
 * Manages several variable-sized entries in one block.
 */
public class VarSizeEntry {

    private final static int MAX_STRING = 50;

    private final VarSizeBlock root;
    private int offset;
    private int entrySize;

    public String toString() {
        Block block = root.getBlock();
        int oldPosition = block.position();
        try {
            if (root.isMultiEntry()) {
                int representationSize = entrySize;
                if (entrySize > MAX_STRING) {
                    representationSize = MAX_STRING;
                }
                byte[] data = new byte[representationSize];
                block.position(offset);
                block.get(data, 0, data.length);
                String content = new String(data);
                if (entrySize > MAX_STRING) {
                    content = content + "...";
                }
                return "VarSizeEntry: b " + block.getAbsoluteIndex() + " off " + offset + " sz " + entrySize + " d " + content;
            } else {
                return "VarSizeEntry: invalid";
            }
        } finally {
            block.position(oldPosition);
        }
    }

    /**
     * Checks if currently selected entry is the first one
     *
     * @return {@code true} if a current entry is the first one in the block
     */
    public boolean isFirst() {
        return offset == root.getHeaderSize();
    }

    /**
     * Checks if currently selected entry is the last one
     *
     * @return {@code true} if a current entry is the last one in the block
     */
    public boolean isLast() {
        if (offset <= 0) {
            return true;
        }
        return offset +entrySize == getTotalSize();
    }

    /**
     * Gets the currently selected entry's size
     * @return the size of the entry
     */
    public int getEntrySize() {
        return entrySize;
    }

    /**
     * Sets the size of the currently selected entry
     * <p>When entry is selected the entry size and offset should be set.</p>
     * @param entrySize the entry size in bytes
     */
    public void setEntrySize(int entrySize) {
        if (entrySize != -1) {
            this.entrySize = entrySize;
        } else {
            this.entrySize = getTotalSize() - offset;
        }
    }

    /**
     * Gets the current offset within a block.
     * <p>It defines where starts the currently selected entry.</p>
     * @return the offset with the mem block in bytes from its beginning
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Sets the current offset within a block
     * <p>It defines where starts the currently selected entry.</p>
     * <p>When entry is selected the entry size and offset should be set.</p>
     * @param offset the offset from the block begin in bytes
     */
    public void setOffset(final int offset) {
        this.offset = offset;
    }

    /**
     * Creates the instance of entry
     * @param root the anchor-block
     */
    public VarSizeEntry(final VarSizeBlock root) {
        this.root = root;
    }

    /**
     * Returns the total size of all elements stored in the block, including entries and header.
     * @return return the total size in bytes
     */
    public int getTotalSize() {
        return getDataLength()+root.getHeaderSize();
    }

    /**
     * Gets the total size of payload stored in the block. It does not contain the header size.
     *
     * @return the data length in bytes
     */
    public int getDataLength() {
        final Block block = root.getBlock();
        block.position(1);
        return (int)block.getLong(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    /**
     * Gets the free space within the current block
     *
     * @return the free space within the block in bytes
     */
    public int getFreeSpace() {
        return getDataCapacity() - getDataLength();
    }

    /**
     * Increments the current data length within the current block
     * @param delta the delta in bytes to increase the length
     */
    public void incDataLength(final long delta) {
        final Block block = root.getBlock();
        block.position(1);
        long value = block.getLong(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        value += delta;
        block.back(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        block.putLong(value, root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    /**
     * Gets the total capacity of the block
     * @return capacity of the block in bytes
     */
    public int getDataCapacity() {
        return root.getDataCapacity();
    }

    /**
     * Compares the currently selected entry with the data provided
     * @param data the data to compare with
     * @return positive if the data is greater, negative - opposite
     */
    public int compare(final byte[] data) {
        final Block block = root.getBlock();
        block.position(offset);
        int len = data.length;
        if (len > entrySize){
            len = entrySize;
        }
        int res = block.compare(data,0,len);
        if (res == 0) {
            return data.length - entrySize;
        } else {
            return res;
        }
    }

    /**
     * Extracts the currently selected entry as a byte array
     * @return the currently selected entry as a byte array
     */
    public byte[] extract() {
        final Block block = root.getBlock();
        block.position(offset);
        final byte[] data = new byte[entrySize];
        block.get(data,0,entrySize);
        return data;
    }

    /**
     * Inserts the data provided to the currently selected offset.
     * <p>The data that is currently located in the block is shifted to make the space for the provided data.</p>
     * @param data the data to insert
     */
    public void insert(final byte[] data) {
        final Block block = root.getBlock();
        int windowSize = data.length;
        int tailSize = getTotalSize()-offset;
        block.position(offset);
        block.insertWindows(windowSize, tailSize);
        block.put(data,0,data.length);
        incDataLength(windowSize);
    }

    /**
     * Expands the block by adding the data provided to the end of the block.
     * <p>Compare to {@link #insert(byte[])}</p>
     * @param data the data to insert
     */
    public void expand(final byte[] data) {
        final Block block = root.getBlock();
        int dataSizeBefore = getTotalSize();
        block.position(dataSizeBefore);
        block.put(data,0, data.length);
        incDataLength(data.length);
        offset = dataSizeBefore;
    }

    /**
     * Removes the currently selected entry from the block
     */
    public void remove() {
        final Block block = root.getBlock();
        int dataSizeBefore = getTotalSize();
        int tailSize = dataSizeBefore-offset-entrySize;
        block.position(offset);
        block.collapseWindow(entrySize, tailSize);
        incDataLength(-entrySize);
    }

    /**
     * Initializes a new entry-based block at the provided block.
     * <p>It will set a proper preamble, offset is put to the first entry</p>
     * @param block the memory block
     */
    public void init(final Block block) {
        block.position(0);
        byte preamble = 0;
        block.putByte(preamble);
        block.putLong(0, root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        offset = root.computeHeaderSize(preamble);
    }

    /**
     * Splits the current block into two at the point of offset.
     * <p>Everything what is after the offset is moved to the next block.</p>
     * <p>This is an auxilary operation that facilitates insert/expand operations in case if the capacity of the block is reached.</p>
     * @param block the newly allocated memory block to move the data to
     */
    public void split(final Block block) {
        int newBlockSize = getTotalSize() - offset;
        block.position(0);
        block.putByte((byte)0);
        block.putLong(newBlockSize, root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        Block sourceBlock = root.getBlock();
        sourceBlock.position(offset);
        block.put(sourceBlock, newBlockSize);
        sourceBlock.position(1);
        sourceBlock.putLong(offset-root.getMultiEntryHeaderSize(), root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    /**
     * Splits the current block into two at the point of offset, adds the provided data at the beginning of the new block and move the rest from the original block afterwards.
     * <p>As a {@link #split(Block)}, but put the data before the split part in the new block.</p>
     *
     * @param block the newly allocated block to move the data to
     * @param data the data to move
     */
    public void split(final Block block, byte[] data) {
        int moveSize = getTotalSize() - offset;
        int newBlockSize = moveSize + data.length;
        block.position(0);
        block.putByte((byte)0);
        block.putLong(newBlockSize, root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        block.put(data, 0, data.length);
        Block sourceBlock = root.getBlock();
        sourceBlock.position(offset);
        block.put(sourceBlock, moveSize);
        sourceBlock.position(1);
        sourceBlock.putLong(offset-root.getMultiEntryHeaderSize(), root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

}
