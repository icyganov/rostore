package org.rostore.v2.container.async;

import org.rostore.entity.Record;

public interface AsyncListener {

    void record(final Record record);
    void error(final Exception e);
    void status(final AsyncStatus asyncStatus);

}
