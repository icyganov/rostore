package org.rostore.v2.catalog;

public interface EntrySizeListener {
    void apply(final long newSize, final long delta);
}
