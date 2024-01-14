package org.rostore.entity.media;

/**
 * Represents information about the version of the rostore instance.
 */
public class Version {
    private String apiVersion;

    public String getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public Version(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    // JSON-B
    public Version() {
    }
}
