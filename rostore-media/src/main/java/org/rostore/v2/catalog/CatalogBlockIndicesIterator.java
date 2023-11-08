package org.rostore.v2.catalog;

import org.rostore.entity.RoStoreException;

public class CatalogBlockIndicesIterator {

    private final CatalogBlockIndices blockIndices;
    private int groupIndex = -1;
    private int inGroupIndex = -1;
    private int position = -1;

    public CatalogBlockIndicesIterator(final CatalogBlockIndices blockIndices) {
        this.blockIndices = blockIndices;
        if (!blockIndices.isEmpty()) {
            groupIndex = 0;
            inGroupIndex = 0;
            position = 0;
        }
    }

    public boolean isEmpty() {
        return blockIndices.isEmpty();
    }

    public boolean isValid() {
        return groupIndex != -1;
    }

    public int position() {
        return position;
    }

    public int left() {
        if (isValid()) {
            return blockIndices.getLength() - position;
        } else {
            return 0;
        }
    }

    private void invalidate() {
        groupIndex=-1;
        position=-1;
        inGroupIndex=-1;
    }

    private long currentGroupLength() {
        long[] group = blockIndices.getGroup(groupIndex);
        return group[1] - group[0] + 1;
    }

    public long get() {
        if (!isValid()) {
            throw new RoStoreException("Get from invalid iterator");
        }
        long value = blockIndices.getGroup(groupIndex)[0] + inGroupIndex;
        inGroupIndex++;
        position++;
        if (inGroupIndex >= currentGroupLength()) {
            groupIndex++;
            inGroupIndex = 0;
            if (groupIndex>=blockIndices.getGroupNumber()) {
                invalidate();
            }
        }
        return value;
    }

}
