package org.rostore.v2.keys;

import org.rostore.Utils;
import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockType;

/**
 * This class is used when the key data exceeds the size of block.
 * <p>In this case a sequence of blocks is allocated to store the data.</p>
 * <p>This class acts like a window to the currently selected block within this sequence.</p>
 * <p>The data is accessed here in the sequential manner.</p>
 */
public class VarSizeMultiBlock {

    /**
     * Absolute index of the block in the storage
     */
    private long currentBlockIndex;
    /**
     * Index of the block within sequence
     */
    private int index;
    private final VarSizeBlock root;

    public String toString() {
        final Block block = root.getBlock();
        int oldPosition = block.position();
        try {
            if (root.isMultiBlock()) {
                long size = getDataSize();
                long representationSize = size;
                if (representationSize > Properties.MAX_STRING_KEY_REPRESENTATION) {
                    representationSize = Properties.MAX_STRING_KEY_REPRESENTATION;
                }
                final String content = toString((int)representationSize);
                String nx = getNextBlockIndex()==0?" x":" nx " + getNextBlockIndex();
                return "MultiBlock: sz " + size + nx + " d " + content;
            } else {
                return "MutliBlock: invalid";
            }
        } finally {
            block.position(oldPosition);
        }
    }

    private String toString(int length) {
        long prevBlockIndex = currentBlockIndex;
        int prevIndex = index;
        try {
            root();
            long dataSize = getDataSize();
            if (length < 0 || length > dataSize) {
                length = (int) dataSize;
            }
            byte[] data = new byte[length];
            root();
            int current = 0;
            while (current != length) {
                Block block = getBlock();
                int dataLength = getBlockDataSize();
                if (current + dataLength > length) {
                    dataLength = length - current;
                }
                block.position(getHeaderSize());
                block.get(data, current, dataLength);
                current += dataLength;
                if (!next()) {
                    break;
                }
            }
            return new String(data) + (dataSize > length ? "...":"");
        } finally {
            currentBlockIndex = prevBlockIndex;
            index = prevIndex;
        }
    }

    /**
     * Creates a multi block
     *
     * @param root the root block
     */
    protected VarSizeMultiBlock(final VarSizeBlock root) {
        this.root = root;
        if (root.valid()) {
            this.currentBlockIndex = root.getBlock().getAbsoluteIndex();
        } else {
            this.currentBlockIndex = -1;
        }
    }

    /**
     * Provides the index of the next block
     * <p>This is only non-zero if there is the next block</p>
     *
     * @return the index of the next block
     */
    public long getNextBlockIndex() {
        final Block block = getBlock();
        block.position(root.getBlockIndex() == currentBlockIndex?1:0);
        return block.getLong(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
    }

    /**
     * Free all the blocks in the multi block sequence
     */
    public void free() {
        root();
        long nextBlock = getNextBlockIndex();
        int nextIndex = index+1;
        root.getBlockProvider().freeBlock(currentBlockIndex);
        while(nextBlock!=0) {
            currentBlockIndex = nextBlock;
            index = nextIndex;
            nextBlock = getNextBlockIndex();
            nextIndex++;
            root.getBlockProvider().freeBlock(currentBlockIndex);
        }
    }

    /**
     * Moves to the next block
     * @return {@code true} if there is new block
     */
    public boolean next() {
        long nextBlockIndex = getNextBlockIndex();
        if (nextBlockIndex==0) {
            return false;
        } else {
            index++;
            currentBlockIndex = nextBlockIndex;
            return true;
        }
    }

    /**
     * Provides a block associated with current position
     *
     * @return the block
     */
    public Block getBlock() {
        return root.getBlockProvider().getBlockContainer().getBlock(currentBlockIndex, BlockType.KEY);
    }

    /**
     * Provides a header size in bytes.
     * <p>For multi block the first block in the sequence will have a greater header than the rest</p>
     *
     * @return the size in bytes
     */
    public int getHeaderSize() {
        if (isRoot()) {
            return root.getHeaderSize();
        } else {
            return root.getBlockProvider().getBlockContainer().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
        }
    }

    /**
     * Identifies if this is the first block in the sequence
     *
     * @return {@code true} if it is the first one
     */
    public boolean isRoot() {
        return currentBlockIndex == root.getBlockIndex();
    }

    /**
     * Provides the total size of the data stored in the multi block
     * @return the size of data in bytes
     */
    public long getDataSize() {
        final Block rootBlock = root.getBlock();
        rootBlock.position(0);
        byte preamble = rootBlock.getByte();
        rootBlock.skip(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        return rootBlock.getLong(preamble);
    }

    /**
     * Stores the key data to the multi block sequence.
     *
     * <p>Blocks are allocated as they requested.</p>
     *
     * @param data the key data to store
     * @return the index of the first block
     */
    public long put(final byte[] data) {
        final Block rootBlock = root.getBlockProvider().allocateBlock(BlockType.KEY);
        final byte preamble = (byte)Utils.computeBytesForMaxValue(data.length);
        rootBlock.putByte(preamble);
        rootBlock.putLong(0, root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        rootBlock.putLong(data.length, preamble);
        final int blockSize = root.getBlockProvider().getMedia().getMediaProperties().getBlockSize();
        int copy = blockSize - rootBlock.position();
        if (copy > data.length) {
            copy = data.length;
        }
        rootBlock.put(data,0, copy);
        Block previousBlock = rootBlock;
        while (copy < data.length) {
            final Block nextBlock = root.getBlockProvider().allocateBlock(BlockType.KEY);
            nextBlock.putLong(0, root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
            if (previousBlock == rootBlock) {
                previousBlock.position(1);
            } else {
                previousBlock.position(0);
            }
            previousBlock.putLong(nextBlock.getAbsoluteIndex(), root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
            int offset = copy;
            copy = offset + blockSize - root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
            if (copy > data.length) {
                copy = data.length;
            }
            nextBlock.put(data, offset, copy-offset);
            previousBlock = nextBlock;
        }
        return rootBlock.getAbsoluteIndex();
    }

    /**
     * Provides a total size of space within the current block
     *
     * @return the capacity of bytes
     */
    public int getDataCapacity() {
        return root.getBlockProvider().getMedia().getMediaProperties().getBlockSize() - getHeaderSize();
    }

    /**
     * Provides the total size stored in the multi block as stored in the header
     * @return the size in bytes
     */
    public long getTotalDataSize() {
        Block block = root.getBlock();
        block.position(0);
        final int preamble = block.getByte();
        block.skip(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        return block.getLong(preamble);
    }

    /**
     * Provides the data stored in this block
     *
     * @return the size in bytes
     */
    public int getBlockDataSize() {
        long totalSize = getTotalDataSize();
        int capacity = getDataCapacity();
        if (isRoot()) {
            if (capacity < totalSize) {
                return capacity;
            }
            return (int)totalSize;
        } else {
            long restDataSize = totalSize - root.getDataCapacity();
            long tailSize = restDataSize - (index-1) * capacity;
            if (tailSize > capacity) {
                return capacity;
            }
            return (int)tailSize;
        }
    }

    /**
     * Compares the provided key data array to the data stored in the multi block sequence
     * @param key the data to be compared with stored
     * @return negative: current < key, positive: current > data, {@code 0} if they are equal
     */
    public int compare(final byte[] key) {
        root();
        long dataSize = getDataSize();
        int offset = 0;
        do {
            int size = getBlockDataSize();
            if (key.length < size + offset) {
                size = key.length - offset;
            }
            final Block block = getBlock();
            block.position(getHeaderSize());
            int res = block.compare(key, offset, size);
            if (res != 0) {
                return res;
            }
            offset += size;
        } while(next() && offset < key.length && offset < dataSize);
        return (int)(key.length - dataSize);
    }

    /**
     * Reads the data from the multi block sequence and stores it to the data array
     *
     * @return the data of the key
     */
    public byte[] get() {
        root();
        final int dataSize = (int) getDataSize();
        final byte[] data = new byte[dataSize];
        int offset = 0;
        do {
            final int size = getBlockDataSize();
            final Block block = getBlock();
            block.position(getHeaderSize());
            block.get(data, offset, size);
            offset += size;
        } while (next() && offset < dataSize);
        return data;
    }

    /**
     * Resets the currently selected block to the first block
     */
    public void root() {
        currentBlockIndex = root.getBlockIndex();
        index = 0;
    }

}
