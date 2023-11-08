package org.rostore.v2.data;

import org.rostore.entity.RoStoreException;

public class DataTransferException extends RoStoreException {
    public DataTransferException(final Exception e) {
        super("Exception while transferring the data", e);
    }

    public DataTransferException(final String message, final Exception e) {
        super(message, e);
    }
}
