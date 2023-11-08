package org.rostore.v2.keys;

import org.rostore.v2.media.block.Block;

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

    public boolean isFirst() {
        return offset == root.getHeaderSize();
    }

    public boolean isLast() {
        if (offset <= 0) {
            return true;
        }
        return offset +entrySize == getTotalSize();
    }

    public int getEntrySize() {
        return entrySize;
    }

    public void setEntrySize(int entrySize) {
        if (entrySize != -1) {
            this.entrySize = entrySize;
        } else {
            this.entrySize = getTotalSize() - offset;
        }
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public VarSizeEntry(final VarSizeBlock root) {
        this.root = root;
    }

    public int getTotalSize() {
        return getDataLength()+root.getHeaderSize();
    }

    public int getDataLength() {
        final Block block = root.getBlock();
        block.position(1);
        return (int)block.getLong(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    public int getFreeSpace() {
        return getDataCapacity() - getDataLength();
    }

    public void incDataLength(long delta) {
        final Block block = root.getBlock();
        block.position(1);
        long value = block.getLong(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        value += delta;
        block.back(root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        block.putLong(value, root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
    }

    public int getDataCapacity() {
        return root.getDataCapacity();
    }

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

    public byte[] extract() {
        final Block block = root.getBlock();
        block.position(offset);
        final byte[] data = new byte[entrySize];
        block.get(data,0,entrySize);
        return data;
    }

    public void insert(final byte[] data) {
        final Block block = root.getBlock();
        int windowSize = data.length;
        int tailSize = getTotalSize()-offset;
        block.position(offset);
        block.insertWindows(windowSize, tailSize);
        block.put(data,0,data.length);
        incDataLength(windowSize);
    }

    public void expand(final byte[] data) {
        final Block block = root.getBlock();
        int dataSizeBefore = getTotalSize();
        block.position(dataSizeBefore);
        block.put(data,0, data.length);
        incDataLength(data.length);
        offset = dataSizeBefore;
    }

    public void remove() {
        final Block block = root.getBlock();
        int dataSizeBefore = getTotalSize();
        int tailSize = dataSizeBefore-offset-entrySize;
        block.position(offset);
        block.collapseWindow(entrySize, tailSize);
        incDataLength(-entrySize);
    }

    public void init(final Block block) {
        block.position(0);
        byte preamble = 0;
        block.putByte(preamble);
        block.putLong(0, root.getBlockProvider().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset());
        offset = root.computeHeaderSize(preamble);
    }

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
     * As a split, but put the data before the split
     *
     * @param block
     * @param data
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
