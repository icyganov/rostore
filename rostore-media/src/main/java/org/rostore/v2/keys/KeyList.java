package org.rostore.v2.keys;

import java.util.ArrayList;
import java.util.List;

/**
 * A list of keys loaded by the {@link KeyBlockOperations}.
 */
public class KeyList {

    private List<byte[]> keys = new ArrayList<>();
    private boolean more = false;
    private long size = 0;

    /**
     * Add a key to the list
     *
     * @param key a key to add
     */
    public void addKey(byte[] key) {
        keys.add(key);
        size+=key.length;
    }

    /**
     * Provides the list of keys
     *
     * @return list of keys as byte arrays
     */
    public List<byte[]> getKeys() {
        return keys;
    }

    /**
     * Sets the list of keys
     *
     * @param keys the list of keys
     */
    public void setKeys(final List<byte[]> keys) {
        this.keys = keys;
    }

    /**
     * Provides a flag if more keys are available in the storage
     *
     * @return {@code true} if more keys are available
     */
    public boolean isMore() {
        return more;
    }

    /**
     * Sets the more flag
     * @param more {@code true} if more keys are available
     */
    public void setMore(final boolean more) {
        this.more = more;
    }

    /**
     * Provides the size of all keys
     * @return the size of all keys
     */
    public long getSize() {
        return size;
    }

    /**
     * Sets the size of all keys
     *
     * @param size the size of all keys in the list
     */
    public void setSize(long size) {
        this.size = size;
    }
}
