package org.rostore.client;

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
