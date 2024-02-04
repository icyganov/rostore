package org.rostore.v2.keys;

import org.rostore.Utils;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockType;

public class VarSizeMultiBlock {

    private long currentBlockIndex;
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

    protected VarSizeMultiBlock(final VarSizeBlock root) {
        this.root = root;
        if (root.valid()) {
            this.currentBlockIndex = root.getBlock().getAbsoluteIndex();
        } else {
            this.currentBlockIndex = -1;
        }
    }

    public long getNextBlockIndex() {
        final Block block = getBlock();
        block.position(root.getBlockIndex() == currentBlockIndex?1:0);
        return block.getLong(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
    }

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
     * @return true if there is new block
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

    public Block getBlock() {
        return root.getBlockProvider().getBlockContainer().getBlock(currentBlockIndex, BlockType.KEY);
    }

    public int getHeaderSize() {
        if (isRoot()) {
            return root.getHeaderSize();
        } else {
            return root.getBlockProvider().getBlockContainer().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex();
        }
    }

    public boolean isRoot() {
        return currentBlockIndex == root.getBlockIndex();
    }

    public int computeRootMultiBlockHeaderSize(long dataSize) {
        return Utils.computeBytesForMaxValue(dataSize) + root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() + 1;
    }

    public int computeRootMultiBlockDataCapacity(long dataSize) {
        return root.getBlockProvider().getMedia().getMediaProperties().getBlockSize() - computeRootMultiBlockHeaderSize(dataSize);
    }

    public long getBlockNumber() {
        root();
        long number = 1;
        while(next()) {
            number++;
        }
        return number;
    }

    public void addIndices(final CatalogBlockIndices catalogBlockIndices) {
        root();
        while(next()) {
            catalogBlockIndices.add(currentBlockIndex,currentBlockIndex);
        }
    }

    public long getDataSize() {
        final Block rootBlock = root.getBlock();
        rootBlock.position(0);
        byte preamble = rootBlock.getByte();
        rootBlock.skip(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        return rootBlock.getLong(preamble);
    }

    public long create(final byte[] data) {
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

    public int getDataCapacity() {
        return root.getBlockProvider().getMedia().getMediaProperties().getBlockSize() - getHeaderSize();
    }

    public long getTotalDataSize() {
        Block block = root.getBlock();
        block.position(0);
        final int preamble = block.getByte();
        block.skip(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex());
        return block.getLong(preamble);
    }

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

    public int compare(byte[] data, int offset, int length) {
        Block block = getBlock();
        block.position(getHeaderSize());
        return block.compare(data,offset,length);
    }

    public void put(byte[] data, int offset, int length) {
        Block block = getBlock();
        block.position(getHeaderSize());
        block.put(data,offset,length);
    }

    public void get(byte[] data, int offset, int length) {
        Block block = getBlock();
        block.position(getHeaderSize());
        block.get(data,offset,length);
    }


    public void root() {
        currentBlockIndex = root.getBlockIndex();
        index = 0;
    }

}
