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

    public void addKey(byte[] key) {
        keys.add(key);
        size+=key.length;
    }

    public List<byte[]> getKeys() {
        return keys;
    }

    public void setKeys(List<byte[]> keys) {
        this.keys = keys;
    }

    public boolean isMore() {
        return more;
    }

    public void setMore(boolean more) {
        this.more = more;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}
