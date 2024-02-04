package org.rostore.v2.media.block.allocator;

public class Properties {

    /**
     * Name of the root allocator
     */
    public static final String ROOT_ALLOCATOR_NAME = "root";

    /**
     * The minimum number of blocks that is allocated in advance in the root allocator's cache
     */
    public static final int ROOT_ALLOCATOR_CACHE_MIN_SIZE = 10;

    /**
     * The maximum number of blocks that is allocated in advance in the root allocator's cache
     */
    public static final int ROOT_ALLOCATOR_CACHE_MAX_SIZE = 100;

    /**
     * The minimum number of blocks that the root allocator should maintain.
     * <p>It is needed to prevent overbooking in parallel processes.</p>
     */
    public static final int ROOT_ALLOCATOR_MIN_BUFFER = 10;

    /**
     * The minimum number of blocks that is allocated in advance in the secondary allocator's cache.
     *
     * <p>Should be 0 otherwise it will start extracting on remove operation.</p>
     */
    public static final int SECONDARY_ALLOCATOR_CACHE_MIN_SIZE = 0;

    /**
     * The maximum number of blocks that is allocated in advance in the secondary allocator's cache
     */
    public static final int SECONDARY_ALLOCATOR_CACHE_MAX_SIZE = 50;

    /**
     * The minimum number of blocks that the secondary allocator should maintain.
     * <p>It is needed to prevent overbooking in parallel processes.</p>
     */
    public static final int SECONDARY_ALLOCATOR_MIN_BUFFER = 5;

}
