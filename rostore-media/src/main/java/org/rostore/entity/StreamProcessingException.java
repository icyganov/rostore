package org.rostore.entity;

public class StreamProcessingException extends RoStoreException {

    public StreamProcessingException(final Exception e) {
        super("Exception in the stream processing", e);
    }
}
