package org.rostore.entity;

/**
 * Exception is thrown by {@link org.rostore.v2.media.block.allocator.BlockAllocator} when the request has to be rejected because
 * the size that has to be allocated is greater than the allowed size
 * of the underlying storage object, e.g. {@link org.rostore.v2.container.Container}.
 */
public class QuotaExceededException extends RoStoreException {
    public QuotaExceededException(final String message) {
        super(message);
    }

    public QuotaExceededException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
