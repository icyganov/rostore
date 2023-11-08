package org.rostore.v2.container.async;

import org.rostore.entity.Record;

public interface AsyncFunction<I> {
    void process(final I stream) throws Exception;
}
