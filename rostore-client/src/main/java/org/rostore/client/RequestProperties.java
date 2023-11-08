package org.rostore.client;

import org.rostore.entity.media.RecordOption;

import java.util.EnumSet;
import java.util.Set;

public class RequestProperties {

    private String contentType;
    private String path;
    private Long version;
    private Long eol;
    private EnumSet<RecordOption> recordOptions = EnumSet.noneOf(RecordOption.class);
    private String trackingId;

    private final RoStoreClientProperties roStoreClientProperties;

    public RoStoreClientProperties getRoStoreClientProperties() {
        return roStoreClientProperties;
    }

    public String getContentType() {
        return contentType;
    }

    public RequestProperties contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public String getPath() {
        return path;
    }

    public Long getEOL() {
        return eol;
    }

    public RequestProperties path(final String path) {
        this.path = path;
        return this;
    }

    public Long version() {
        return version;
    }

    public RequestProperties version(final Long version) {
        this.version = version;
        return this;
    }

    public RequestProperties unixEol(final Long eol) {
        this.eol = eol;
        return this;
    }

    public RequestProperties options(final EnumSet<RecordOption> recordOptions) {
        if (recordOptions == null) {
            this.recordOptions = EnumSet.noneOf(RecordOption.class);
        } else {
            this.recordOptions = recordOptions;
        }
        return this;
    }

    public EnumSet<RecordOption> getRecordOptions() {
        return recordOptions;
    }

    public String trackingId() {
        return trackingId;
    }

    public RequestProperties trackingId(final String trackingId) {
        this.trackingId = trackingId;
        return this;
    }

    public RequestProperties(final RoStoreClientProperties roStoreClientProperties) {
        this.roStoreClientProperties = roStoreClientProperties;
    }
}
