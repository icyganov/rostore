package org.rostore.v2.seq;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.catalog.CatalogBlockIndicesIterator;
import org.rostore.v2.media.RootClosableImpl;
import org.rostore.v2.media.block.Block;
import org.rostore.v2.media.block.BlockProvider;
import org.rostore.v2.media.block.BlockType;

import java.util.function.Function;

/**
 * Sequences manage a set of blocks. It is needed to be able to manage an object that is longer than one block.
 * <p>This object is used to manage a dynamic object, where the blocks can be added and removed from.</p>
 * <p>There are two major cases cases for the block sequence usage: {@link org.rostore.v2.catalog.CatalogBlockOperations}
 * and {@link org.rostore.v2.keys.KeyBlockOperations}.</p>
 *
 * <p>Catalog block operations manages the list of blocks, that allow to build {@link org.rostore.v2.media.block.allocator.BlockAllocator}
 * that accounting all the blocks in the media or some part of it.</p>
 * <p>Key block operations manage the set of keys.</p>
 *
 * <p>This entity is an active counterpart of {@link BlockIndexSequence}, which only manages the
 * sequence, when this object adds all the functionality of {@link SequenceBlock}, that
 * implements block data manipulation.</p>
 *
 * @param <T> a type of the block with the functionality to access its content, it depends on the type of sequence
 */
public class BlockSequence<T extends SequenceBlock> extends RootClosableImpl {

    final private BlockIndexSequence blockIndexSequence;
    private final T sequenceBlock;

    private final BlockProvider blockProvider;

    private final BlockType blockType;

    public BlockProvider getBlockProvider() {
        return blockProvider;
    }

    public T getSequenceBlock() {
        return sequenceBlock;
    }

    public int length() {
        return blockIndexSequence.getFirstFreeIndex();
    }

    /**
     * Because the sequences can be used in the allocators, it can't rely all the time on the
     * external allocators, as they might use the same sequence as this one, which would lead
     * to a concurrent operation caused by the allocator. That's why the sequence uses a
     * preallocated set of blocks (free blocks), that can be used during its operation. After the
     * operation is over the sequence should be "rebalanced" to reinstate the number of free
     * blocks that it might use in the next operation.
     */
    public void rebalance() {
        checkOpened();
        int freeNumber = blockIndexSequence.length() - blockIndexSequence.getFirstFreeIndex();
        if (freeNumber < Properties.MIN_FREE_BLOCK_NUMBER) {
            int alloc = Properties.AVG_FREE_BLOCK_NUMBER - freeNumber;
            addFreeBlocks(alloc);
            return;
        }

        if (freeNumber > Properties.MAX_FREE_BLOCK_NUMBER) {
            int free = freeNumber - Properties.AVG_FREE_BLOCK_NUMBER;
            removeFreeBlocks(free);
        }
    }

    private void removeFreeBlocks(final int numberOfBlocks) {
        int freeStartIndex = blockIndexSequence.length() - numberOfBlocks;
        CatalogBlockIndices toFree = new CatalogBlockIndices();
        for(int i=freeStartIndex; i<blockIndexSequence.length(); i++) {
            long blockIndex = getBlockByIndex(i).getAbsoluteIndex();
            toFree.add(blockIndex, blockIndex);
        }
        blockIndexSequence.removeAtEnd(numberOfBlocks);
        Block block = getBlockByIndex(blockIndexSequence.length()-1);
        block.position(0);
        block.writeBlockIndex(0);
        blockProvider.getBlockAllocator().getBlockAllocatorInternal().free(toFree, false);
    }

    private void addFreeBlocks(final int numberOfBlocks) {
        CatalogBlockIndices toAdd = blockProvider.getBlockAllocator().getBlockAllocatorInternal().allocate(blockType, numberOfBlocks, false);
        CatalogBlockIndicesIterator indices = toAdd.iterator();
        int index = blockIndexSequence.length();
        Block prevBlock = getBlockByIndex(blockIndexSequence.length()-1);
        while (indices.left() != 0) {
            final long blockIndex = indices.get();
            blockIndexSequence.add(index, blockIndex);
            final Block block = blockProvider.getBlockContainer().getBlock(blockIndex, blockType);
            sequenceBlock.moveTo(index);
            sequenceBlock.clean();
            if (prevBlock != null) {
                prevBlock.position(0);
                prevBlock.writeBlockIndex(blockIndex);
            }
            index++;
            prevBlock = block;
        }
    }

    /**
     * Inserts a new block after the given index in the sequence.
     *
     * <p>This operation is a self-allocation logic. Block sequence selects any free
     * block preallocated in the sequence and adds it to the specified location.</p>
     *
     * <p>This added block can be used after this operation.</p>
     *
     * @param afterIndex the index in the sequence after which a free block should be inserted
     */
    public void addFreeBlock(final int afterIndex) {
        checkOpened();
        if (afterIndex == blockIndexSequence.getFirstFreeIndex() - 1) {
            // this is the last index
            blockIndexSequence.setFirstFreeIndex(blockIndexSequence.getFirstFreeIndex()+1);
            return;
        }
        Block beforeBlock = getBlockByIndex(afterIndex);
        Block freeBlock = getBlockByIndex(blockIndexSequence.getFirstFreeIndex());

        Block preFreeBlock = getBlockByIndex(blockIndexSequence.getFirstFreeIndex()-1);
        freeBlock.position(0);
        long nextAfterFree = freeBlock.readBlockIndex();
        preFreeBlock.position(0);
        preFreeBlock.writeBlockIndex(nextAfterFree);

        beforeBlock.position(0);
        long nextBlock = beforeBlock.readBlockIndex();
        beforeBlock.backBlockIndex();
        beforeBlock.writeBlockIndex(freeBlock.getAbsoluteIndex());
        freeBlock.position(0);
        freeBlock.writeBlockIndex(nextBlock);
        blockIndexSequence.remove(blockIndexSequence.getFirstFreeIndex());
        blockIndexSequence.add(afterIndex+1, freeBlock.getAbsoluteIndex());
        blockIndexSequence.setFirstFreeIndex(blockIndexSequence.getFirstFreeIndex()+1);
    }

    /**
     * Operation marks the block with the given index as a free one, assuming
     * previously it was used by the data.
     *
     * <p>This is self-allocation operation, that allows to mark a block
     * as free without involving any allocator. A sequence
     * manages treats these blocks as free.</p>
     *
     * @param index the index of the block in the sequence to mark as free
     */
    public void removeFreeBlock(final int index) {
        checkOpened();
        if (index == 0) {
            throw new RoStoreException("Can't free the first block");
        }
        if (index == blockIndexSequence.getFirstFreeIndex() - 1) {
            // this is the last index
            blockIndexSequence.setFirstFreeIndex(blockIndexSequence.getFirstFreeIndex()-1);
            sequenceBlock.moveTo(blockIndexSequence.getFirstFreeIndex());
            sequenceBlock.clean();
            return;
        }

        Block freeBlock = getBlockByIndex(index);
        freeBlock.position(0);
        long nextBlock = freeBlock.readBlockIndex();
        Block beforeBlock = getBlockByIndex(index-1);
        beforeBlock.position(0);
        beforeBlock.writeBlockIndex(nextBlock);
        blockIndexSequence.remove(index);
        blockIndexSequence.setFirstFreeIndex(blockIndexSequence.getFirstFreeIndex()-1);

        Block lastBlock = getBlockByIndex(blockIndexSequence.length()-1);
        blockIndexSequence.add(blockIndexSequence.length(), freeBlock.getAbsoluteIndex());
        lastBlock.position(0);
        lastBlock.writeBlockIndex(freeBlock.getAbsoluteIndex());
        sequenceBlock.moveTo(blockIndexSequence.length()-1);
        sequenceBlock.clean();
    }

    /**
     * Create a new sequence.
     *
     * @param blockProvider a block provider that will be used internally to get access to the blocks
     * @param catalogBlockIndices a set of blocks where the sequence should be stored
     * @param factory a factory to create a {@link SequenceBlock}, which will hold data manipulation logic on the blocks of sequence
     * @param blockType a type of the blocks in the sequence
     */
    public BlockSequence(final BlockProvider blockProvider,
                         final CatalogBlockIndices catalogBlockIndices,
                         final Function<BlockSequence<T>, T> factory,
                         final BlockType blockType) {
        this.blockType = blockType;
        this.blockProvider = blockProvider;
        blockIndexSequence = new BlockIndexSequence();
        sequenceBlock = factory.apply(this);
        CatalogBlockIndicesIterator indices = catalogBlockIndices.iterator();
        int index = 0;
        Block prevBlock = null;
        while (indices.left() != 0) {
            final long blockIndex = indices.get();
            blockIndexSequence.add(index, blockIndex);
            Block block = blockProvider.getBlockContainer().getBlock(blockIndex, blockType);
            sequenceBlock.moveTo(index);
            sequenceBlock.clean();
            index++;
            if (prevBlock != null) {
                prevBlock.position(0);
                prevBlock.writeBlockIndex(blockIndex);
            }
            prevBlock = block;
        }
        blockIndexSequence.setFirstFreeIndex(1);
        blockIndexSequence.markSequenceUsed();
    }

    /**
     * Creates block sequence from cached blockIndex sequence
     * @param blockProvider
     * @param blockIndexSequence
     * @param factory
     * @param blockType
     */
    public BlockSequence(final BlockProvider blockProvider,
                         final BlockIndexSequence blockIndexSequence,
                         final Function<BlockSequence<T>, T> factory,
                         final BlockType blockType) {
        this.blockType = blockType;
        this.blockProvider = blockProvider;
        this.blockIndexSequence = blockIndexSequence;
        this.blockIndexSequence.markSequenceUsed();
        this.sequenceBlock = factory.apply(this);
    }

    /** load existing **/
    public BlockSequence(final BlockProvider blockProvider,
                         long startBlockIndex,
                         final Function<BlockSequence<T>, T> factory,
                         final BlockType blockType) {
        this.blockType = blockType;
        this.blockProvider = blockProvider;
        blockIndexSequence = new BlockIndexSequence();
        this.sequenceBlock = factory.apply(this);
        Block block = blockProvider.getBlockContainer().getBlock(startBlockIndex, blockType);
        int firstFreeIndex = -1;
        int index = 0;
        while (block!=null) {
            blockIndexSequence.add(index, block.getAbsoluteIndex());
            final long nextBlck = block.readBlockIndex();
            sequenceBlock.moveTo(index);
            if (firstFreeIndex == -1 && sequenceBlock.isUnused()) {
                firstFreeIndex = index;
            }
            if (nextBlck != 0) {
                block = blockProvider.getBlockContainer().getBlock(nextBlck, blockType);
            } else {
                block = null;
            }
            index++;
        }
        if (firstFreeIndex == -1) {
            firstFreeIndex = index;
        }
        blockIndexSequence.setFirstFreeIndex(firstFreeIndex==0?1:firstFreeIndex);
        blockIndexSequence.markSequenceUsed();
    }

    /**
     * Provides a block by the sequence number, starting from 0
     *
     * @param seqIndex the index in the sequence
     * @return the block from the sequence
     */
    public Block getBlockByIndex(final int seqIndex) {
        checkOpened();
        return blockProvider.getBlockContainer().getBlock(blockIndexSequence.getBlockIndex(seqIndex), blockType);
    }

    /**
     * Provides the underlying block index sequence
     * @return the block index sequence
     */
    public BlockIndexSequence getBlockIndexSequence() {
        return blockIndexSequence;
    }

    /**
     * Closes an instance of the block sequence, and
     * decrements the number of references in {@link BlockIndexSequence}.
     * <p>Once the number of references in {@link BlockIndexSequence} is 0, the block index sequence can be garbage collected.</p>
     */
    @Override
    public void close() {
        super.close();
        blockIndexSequence.close();
    }

}
