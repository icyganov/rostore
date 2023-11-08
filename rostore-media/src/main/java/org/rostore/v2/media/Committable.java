package org.rostore.v2.media;

public interface Committable extends Closeable {
    void commit();
}
