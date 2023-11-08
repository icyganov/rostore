package org.rostore.entity;

import org.rostore.v2.keys.KeyList;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StringKeyList {

    private List<String> keys = new ArrayList<>();
    private boolean more = false;

    public StringKeyList() {
    }

    public StringKeyList(final KeyList keyList) {
        for(byte[] key : keyList.getKeys()) {
            keys.add(new String(key, StandardCharsets.UTF_8));
        }
        more = keyList.isMore();
        if (!more) {
            Collections.sort(keys);
        }
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public boolean isMore() {
        return more;
    }

    public void setMore(boolean more) {
        this.more = more;
    }
}
