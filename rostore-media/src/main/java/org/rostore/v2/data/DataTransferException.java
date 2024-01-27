package org.rostore.v2.data;

import org.rostore.entity.RoStoreException;

/**
 * Thrown when data transfer is aborted out of any reason
 */
public class DataTransferException extends RoStoreException {
    public DataTransferException(final Exception e) {
        super("Exception while transferring the data", e);
    }

    public DataTransferException(final String message, final Exception e) {
        super(message, e);
    }
}
