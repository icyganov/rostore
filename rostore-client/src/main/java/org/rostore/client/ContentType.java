package org.rostore.client;

/**
 * Specify a content-type header for the request to the remote rostore service.
 */
public enum ContentType {
    JSON("application/json"),
    BINARY("application/octet-stream");

    private String mediaType;

    public String getMediaType() {
        return mediaType;
    }

    ContentType(final String mediaType) {
        this.mediaType = mediaType;
    }
}
