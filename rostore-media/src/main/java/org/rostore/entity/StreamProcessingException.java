package org.rostore.entity;

/**
 * Used in async stream processing function
 */
public class StreamProcessingException extends RoStoreException {

    /**
     * Creates an exception by wrapping another one
     * @param e the exception to wrap
     */
    public StreamProcessingException(final Exception e) {
        super("Exception in the stream processing", e);
    }
}
