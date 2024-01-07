package org.rostore.client;

import java.util.ArrayList;
import java.util.List;

/**
 * The class represents the result of key listing operation.
 *
 * <p>This operation is paginated, and any request will return only a portion of
 * the entries. The client may decide to send a new request to continue listing.
 * </p>
 * <p>The sorting of the keys in the list returned is not guaranteed.</p>
 * <p>If {@link #isMore()} is true, there are more keys in the storage, and the pagination
 * is expected to return more results.</p>
 * <p>To continue the listing the {@link #getContinuationKey()} should be used.</p>
 */
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
