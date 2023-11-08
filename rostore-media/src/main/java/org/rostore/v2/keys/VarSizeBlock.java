package org.rostore.v2.keys;

import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.BlockType;

/**
 * Header:
 * number of bytes      | what
 * ----------------------------
 * 1                    | preamble: number of bytes for length encoding (multi) or 0
 * ----------------------------
 * preamble = 1..4 (multi-block)
 * ----------------------------
 * (bytesPerBlockIndex) | next block index
 * (preamble)           | length of the data
 * ----------------------------
 * preamble = 0 (multi-entries)
 * ----------------------------
 * (bytesPerBlockOffset) | length of data
 */
public class VarSizeBlock {

    private final VarSizeMultiBlock multiBlock;
    private final VarSizeEntry multiEntry;
    private long blockIndex;

    private final BlockProvider blockProvider;

    public boolean invalid() {
        return blockIndex == -1;
    }

    public boolean valid() {
        return blockIndex != -1;
    }
    
    public String toString() {
        if (invalid()) {
            return "VarSizeBlock: invalid";
        }
        final Block block = getBlock();
        int backPosition = block.position();
        try {
            if (isMultiBlock()) {
                return multiBlock.toString();
            } else {
                return multiEntry.toString();
            }
        } finally {
            block.position(backPosition);
        }
    }

    public VarSizeMultiBlock getMultiBlock() {
        return multiBlock;
    }

    public VarSizeEntry getEntry() {
        return multiEntry;
    }

    public int computeHeaderSize(byte preamble) {
        if (preamble != 0) {
            // multi-block
            return 1 + blockProvider.getBlockContainer().getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockIndex() + preamble;
        } else {
            return 1 + blockProvider.getMedia().getMediaProperties().getMapperProperties().getBytesPerBlockOffset();
        }
    }

    public BlockProvider getBlockProvider() {
        return blockProvider;
    }

    public int getMultiEntryHeaderSize() {
        return computeHeaderSize((byte)0);
    }

    public int getHeaderSize() {
        byte preamble = getPreamble();
        return computeHeaderSize(preamble);
    }

    protected byte getPreamble() {
        final Block block = getBlock();
        block.position(0);
        return block.getByte();
    }

    public boolean isMultiBlock() {
        return getPreamble() != 0;
    }

    public boolean isMultiEntry() {
        return getPreamble() == 0;
    }

    public Block getBlock() {
        return blockProvider.getBlockContainer().getBlock(blockIndex, BlockType.KEY);
    }

    public long getBlockIndex() {
        return blockIndex;
    }

    public int getDataCapacity() {
        return blockProvider.getMedia().getMediaProperties().getBlockSize() - getHeaderSize();
    }

    public int compare(final byte[] key) {
        if (isMultiBlock()) {
            multiBlock.root();
            long dataSize = multiBlock.getDataSize();
            int offset = 0;
            do {
                int size = multiBlock.getBlockDataSize();
                if (key.length < size + offset) {
                    size = key.length - offset;
                }
                int res = multiBlock.compare(key, offset, size);
                if (res != 0) {
                    return res;
                }
                offset += size;
            } while(multiBlock.next() && offset < key.length && offset < dataSize);
            return (int)(key.length - dataSize);
        } else {
            return multiEntry.compare(key);
        }
    }

    public byte[] extract() {
        if (isMultiBlock()) {
            multiBlock.root();
            int dataSize = (int)multiBlock.getDataSize();
            byte[] data = new byte[dataSize];
            int offset = 0;
            do {
                int size = multiBlock.getBlockDataSize();
                multiBlock.get(data, offset, size);
                offset += size;
            } while(multiBlock.next() && offset < dataSize);
            return data;
        } else {
            return multiEntry.extract();
        }
    }

    public void moveTo(final long blockIndex) {
        this.blockIndex = blockIndex;
    }

    public VarSizeBlock(final BlockProvider blockProvider) {
        this.blockProvider = blockProvider;
        blockIndex = -1;
        this.multiBlock = new VarSizeMultiBlock(this);
        this.multiEntry = new VarSizeEntry(this);
    }
}
