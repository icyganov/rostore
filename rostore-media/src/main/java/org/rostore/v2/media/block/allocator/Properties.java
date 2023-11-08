package org.rostore.v2.media.block.allocator;

public class Properties {

    public static final String ROOT_ALLOCATOR_NAME = "root";
    /**
     * The size of blocks that is allocated in advance in the root allocator (in blocks)
     */
    public static final int ROOT_ALLOCATOR_CACHE_MIN_SIZE = 10;
    public static final int ROOT_ALLOCATOR_CACHE_MAX_SIZE = 100;

    public static final int ROOT_ALLOCATOR_MIN_BUFFER = 10;

    /**
     * The size of blocks that is allocated in advance (in blocks)
     */
    public static final int SECONDARY_ALLOCATOR_CACHE_MIN_SIZE = 0; // should be 0 otherwise it will start extracting on remove operation
    public static final int SECONDARY_ALLOCATOR_CACHE_MAX_SIZE = 50;

    public static final int SECONDARY_ALLOCATOR_MIN_BUFFER = 5;

}
