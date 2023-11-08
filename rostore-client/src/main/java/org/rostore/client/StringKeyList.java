package org.rostore.client;

import java.util.ArrayList;
import java.util.List;

public class StringKeyList {

    private List<String> keys = new ArrayList<>();
    private boolean more = false;

    public StringKeyList() {
    }

    /**
     * List of keys.
     * <p>List of keys can be incomplete, it is also generally not sorted.</p>
     *
     * @return list keys
     */
    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    /**
     *
     * @return {@code true} if there are more entries
     */
    public boolean isMore() {
        return more;
    }

    public void setMore(boolean more) {
        this.more = more;
    }

    /**
     * Provides a continuation key for the next call
     * @return the continuation key or {@code null}
     */
    public String getContinuationKey() {

        if (isMore()) {
            if (keys.size() > 0) {
                return keys.get(keys.size()-1);
            }
        }
        return null;
    }
}
