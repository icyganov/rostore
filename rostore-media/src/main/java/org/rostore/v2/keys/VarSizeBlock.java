package org.rostore.v2.keys;

import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.BlockType;

/**
 * This class refer the sequence of entries of variable size, that are stores in the memory blocks.
 *
 * <p>This block can be set to get the data from the memory blocks in form of entries.</p>
 *
 * <p>Each entry can stretch across several memory blocks (multi-block), and could also be several entries in one
 * memory block, depending on the sizes of these entries.</p>
 *
 * <p>In case of multi-block the memory blocks that store the entry are connected to the main one of the sequence.</p>
 *
 * <p>All the entries in such blocks are keys and sorted alphanumerically.</p>
 *
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

    /**
     * Get the multi block.
     * <p>This one should be used only if the block is of multi-block type, check it by {@link #isMultiBlock()}</p>
     * <p>It is mutually exclusive to {@link #getEntry()}</p>
     * @return the multi-block element
     */
    public VarSizeMultiBlock getMultiBlock() {
        return multiBlock;
    }

    /**
     * Get the entries in the block
     *
     * <p>This one should be used in the multi-entry case, check it by {@link #isMultiEntry()} ()}</p>
     * <p>It is mutually exclusive to {@link #getMultiBlock()}</p>
     * @return the var size entry
     */
    public VarSizeEntry getEntry() {
        return multiEntry;
    }

    public int computeHeaderSize(final byte preamble) {
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

    /**
     * Get the header size of the current block
     * <p>Header is a non-payload administrative unit of memory.</p>
     * @return the size of the header in bytes
     */
    public int getHeaderSize() {
        final byte preamble = getPreamble();
        return computeHeaderSize(preamble);
    }

    /**
     * Get a byte of preamble that defines a type of the block and encode a number of bytes needed to store the length of data in case of multi-block
     * @return a value stored in the preamble
     */
    protected byte getPreamble() {
        final Block block = getBlock();
        block.position(0);
        return block.getByte();
    }

    /**
     * If it is a multi-block block.
     * <p>It should contain several memory blocks to store the entry data.</p>
     * @return {@code true} if this block refers the multi-block
     */
    public boolean isMultiBlock() {
        return getPreamble() != 0;
    }

    /**
     * If it is a multi-entry block (several entries in one block)
     * @return {@code true} if this block refers the multi-entry
     */
    public boolean isMultiEntry() {
        return getPreamble() == 0;
    }

    public Block getBlock() {
        return blockProvider.getBlockContainer().getBlock(blockIndex, BlockType.KEY);
    }

    /**
     * Gets the currently selected block index within the sequence, starting from zero.
     *
     * @return the block index
     */
    public long getBlockIndex() {
        return blockIndex;
    }

    /**
     * Gets the total capacity in bytes of this block
     * <p>It also considers the header's size.</p>
     * @return the total payload size
     */
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

    /**
     * Gets the data payload for currently selected entry
     *
     * @return the payload
     */
    public byte[] get() {
        if (isMultiBlock()) {
            return multiBlock.get();
        } else {
            return multiEntry.extract();
        }
    }

    /**
     * Selects the current block within sequence.
     * <p>Function does not validate the block index</p>
     *
     * @param blockIndex the block index to select
     */
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
