package org.rostore.v2.keys;

import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.BlockType;

/**
 * <table>
 * <caption>Header</caption>
 * <tr><th>number of bytes</th><th>what</th></tr>
 * <tr><td>1</td><td>preamble: number of bytes for length encoding (multi) or 0</td></tr>
 * <tr><td><b>preamble = 1..4 (multi-block)</b></td></tr>
 * <tr><td>(bytesPerBlockIndex)</td><td>next block index</td></tr>
 * <tr><td>(preamble)</td><td>length of the data</td></tr>
 * <tr><td><b>preamble = 0 (multi-entries)</b></td></tr>
 * <tr><td>(bytesPerBlockOffset)</td><td>length of data</td></tr>
 * </table>
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
            return multiBlock.compare(key);
        } else {
            return multiEntry.compare(key);
        }
    }

    public byte[] get() {
        if (isMultiBlock()) {
            return multiBlock.get();
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
