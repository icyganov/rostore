package org.rostore.v2.seq;

public class Properties {

    /**
     * Minimum number of free blocks that the sequence should keep for the
     * internal purposes of on-the-fly allocation.
     *
     * <p>If after some operation the sequence would be left with less than this number,
     * a special rebalancing will be executed to allocate the blocks.</p>
     */
    public static final int MIN_FREE_BLOCK_NUMBER = 3;

    /**
     * Maximum number of free blocks that the sequence should keep for the
     * internal purposes of on-the-fly allocation.
     *
     * <p>If after some operation the sequence would be left with more than this number,
     * a special rebalancing will be executed to remove the blocks from the sequence.</p>
     */
    public static final int MAX_FREE_BLOCK_NUMBER = 5;

    /**
     * The expected number of free blocks the sequence should preferably have.
     *
     * <p>Used when the sequence is created or rebalancing is executed.</p>
     */
    public static final int AVG_FREE_BLOCK_NUMBER = (MIN_FREE_BLOCK_NUMBER + MAX_FREE_BLOCK_NUMBER) / 2;
}
