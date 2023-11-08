package org.rostore.entity.media;

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
