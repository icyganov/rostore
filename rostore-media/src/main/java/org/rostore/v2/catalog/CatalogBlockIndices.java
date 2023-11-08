package org.rostore.v2.catalog;

import org.rostore.entity.RoStoreException;

import java.util.ArrayList;
import java.util.List;

public class CatalogBlockIndices {

    private static final int START = 0;
    private static final int STOP = 1;

    private int length = 0;
    private List<long[]> blockEntries = new ArrayList<>();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean added = false;
        for(long[] entry : blockEntries) {
            if (added) {
                sb.append(",");
            }
            sb.append(entry[START]);
            sb.append("..");
            sb.append(entry[STOP]);
            added = true;
        }
        sb.append("}");
        return sb.toString();
    }

    public void clear() {
        blockEntries.clear();
        length = 0;
    }

    public void add(final long startIndex, final long stopIndex) {
        if (startIndex > stopIndex) {
            throw new RoStoreException("Start index " + startIndex + " > " + stopIndex + " stop index.");
        }
        insert(startIndex, stopIndex);
        length+=stopIndex-startIndex+1;
    }

    public CatalogBlockIndices remove(final long startIndex, final long stopIndex) {
        if (startIndex > stopIndex) {
            throw new RoStoreException("Start index " + startIndex + " > " + stopIndex + " stop index.");
        }
        return removeStartStop(startIndex, stopIndex);
    }

    /**
     * Removes what is included in this catalog
     * @param catalogBlockIndices
     * @return what were not removed
     */
    public CatalogBlockIndices remove(final CatalogBlockIndices catalogBlockIndices) {
        CatalogBlockIndices notRemoved = new CatalogBlockIndices();
        for(int i=0; i<catalogBlockIndices.blockEntries.size(); i++) {
            notRemoved.add(removeStartStop(catalogBlockIndices.blockEntries.get(i)[START], catalogBlockIndices.blockEntries.get(i)[STOP]));
        }
        return notRemoved;
    }

    private long[] wrap(final long start, final long stop) {
        return new long[] {start, stop};
    }

    private void insert(final long start, final long stop) {
        if (blockEntries.isEmpty()) {
            blockEntries.add(wrap(start, stop));
            return;
        }
        long first = blockEntries.get(0)[START];
        if (stop < first) {
            if (first == stop+1) {
                blockEntries.get(0)[START] = start;
            } else {
                blockEntries.add(0, wrap(start, stop));
            }
            return;
        }
        long last = blockEntries.get(blockEntries.size()-1)[STOP];
        if (last<start) {
            if (last == start-1) {
                blockEntries.get(blockEntries.size()-1)[STOP] = stop;
            } else {
                blockEntries.add(wrap(start, stop));
            }
            return;
        }
        for(int i=1; i<blockEntries.size(); i++) {
            long[] current = blockEntries.get(i);
            if (current[START]>stop) {
                long[] prev = blockEntries.get(i-1);
                if (current[START] == stop+1) {

                    if (prev[STOP] == start-1) {
                        prev[STOP] = current[STOP];
                        blockEntries.remove(i);
                    } else {
                        current[START] = start;
                    }
                    return;
                }
                if (prev[STOP] == start-1) {
                    prev[STOP] = stop;
                    return;
                }
                blockEntries.add(i, wrap(start, stop));
                return;
            }
        }
    }

    private CatalogBlockIndices removeStartStop(long start, long stop) {
        CatalogBlockIndices ret = new CatalogBlockIndices();
        if (blockEntries.isEmpty()) {
            ret.add(start, stop);
            return ret;
        }
        int startIndex = -1;
        int stopIndex = -1;
        long[] current;
        for(int i=0; i<blockEntries.size(); i++) {
            current = blockEntries.get(i);
            if (current[STOP] >= start) {
                if (current[START] <= stop) {
                    if (startIndex == -1) {
                        startIndex = i;
                    }
                    stopIndex = i;
                } else {
                    break;
                }
            }
        }
        if (startIndex == -1) {
            // there is no entry matching
            ret.add(start, stop);
            return ret;
        }

        current = blockEntries.get(startIndex);
        if (current[START] > start) {
            ret.add(start, current[START] - 1);
            start = current[START];
        }
        current = blockEntries.get(stopIndex);
        if (current[STOP] < stop) {
            ret.add(current[STOP]+1, stop);
            stop = current[STOP];
        }

        if (startIndex == stopIndex) {
            current = blockEntries.get(stopIndex);
            if (start>current[START]) {
                if (stop==current[STOP]) {
                    length -= current[STOP] - start + 1;
                    current[STOP] = start-1;
                } else {
                    length -= stop - start + 1;
                    long prevStop = current[STOP];
                    current[STOP] = start-1;
                    long[] newOne = new long[] {stop+1, prevStop};
                    blockEntries.add(startIndex+1, newOne);
                }
            } else {
                // start == current[START]
                if (stop == current[STOP]) {
                    length -= stop - start + 1;
                    blockEntries.remove(startIndex);
                } else {
                    length -= stop - current[START] + 1;
                    current[START] = stop+1;
                }
            }
            return ret;
        }

        for (int i=startIndex; i<stopIndex; i++) {
            long[] left = blockEntries.get(i);
            long[] right = blockEntries.get(i+1);
            ret.add(left[STOP]+1, right[START]-1);
        }
        current = blockEntries.get(stopIndex);
        if (current[STOP] > stop) {
            length -= stop - current[START] + 1;
            current[START] = stop+1;
        } else {
            length -= blockEntries.get(stopIndex)[STOP] - blockEntries.get(stopIndex)[START] + 1;
            blockEntries.remove(stopIndex);
        }
        stopIndex--;

        current = blockEntries.get(startIndex);
        if (current[START] < start) {
            length -= current[STOP] - start + 1;
            current[STOP] = start-1;
        } else {
            length -= blockEntries.get(startIndex)[STOP] - blockEntries.get(startIndex)[START] + 1;
            blockEntries.remove(startIndex);
        }
        startIndex++;
        for(int i=startIndex; i<=stopIndex; i++) {
            length -= blockEntries.get(i)[STOP] - blockEntries.get(i)[START] + 1;
            blockEntries.remove(startIndex);
        }
        return ret;
    }

    public void add(final CatalogBlockIndices catalogBlockIndices) {
        for(int i=0; i<catalogBlockIndices.getGroupNumber(); i++) {
            add(catalogBlockIndices.getGroup(i)[START], catalogBlockIndices.getGroup(i)[STOP]);
        }
    }

    public CatalogBlockIndices extract(final int length) {
        if (length > this.length) {
            throw new RoStoreException("Can't extract " + length + " from the the block of " + this.length);
        }
        int left = length;
        CatalogBlockIndices ret = new CatalogBlockIndices();
        while (left != 0) {
            long[] last = blockEntries.get(blockEntries.size()-1);
            long lastLength = last[STOP] - last[START] + 1;
            if (lastLength <= left) {
                ret.add(last[START], last[STOP]);
                blockEntries.remove(blockEntries.size()-1);
                left -= lastLength;
                this.length -= lastLength;
            } else {
                ret.add(last[STOP] - left + 1, last[STOP]);
                last[STOP] = last[STOP] - left;
                this.length -= left;
                left = 0;
            }
        }
        return ret;
    }

    public int getLength() {
        return length;
    }

    public boolean isEmpty() {
        return blockEntries.isEmpty();
    }

    public CatalogBlockIndicesIterator iterator() {
        return new CatalogBlockIndicesIterator(this);
    }

    public long getFirst() {
        if (blockEntries.isEmpty()) {
            throw new IndexOutOfBoundsException("The index is empty.");
        }
        return blockEntries.get(0)[START];
    }

    public long[] getGroup(final int groupIndex) {
        return blockEntries.get(groupIndex);
    }

    public int getGroupNumber() {
        return blockEntries.size();
    }

    public boolean contains(final long blockIndex) {
        for(int i=0; i<blockEntries.size(); i++) {
            long[] entry = blockEntries.get(i);
            if (entry[START] <= blockIndex && entry[STOP] >= blockIndex) {
                return true;
            }
        }
        return false;
    }

}
