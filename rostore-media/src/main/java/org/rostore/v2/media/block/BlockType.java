package org.rostore.v2.media.block;

/**
 * A type of the block. It is needed to decide on the priority of the block,
 * but also is used to validate internal logic.
 *
 * <p>This data is not persisted, and rather is available based on the run-time
 * logic after the block is allocated.</p>
 */
public enum BlockType {

    /**
     * Block belongs to the block catalog and used for block accounting (e.g. {@link org.rostore.v2.catalog.CatalogBlockOperations}).
     */
    CATALOG,
    /**
     * Block belongs to the key catalog and used for block accounting (e.g. {@link org.rostore.v2.keys.KeyBlockOperations}).
     */
    KEY,
    /**
     * This is the value data block, allocated by {@link org.rostore.v2.data.DataWriter} or {@link org.rostore.v2.data.DataReader}.
     */
    DATA
}
