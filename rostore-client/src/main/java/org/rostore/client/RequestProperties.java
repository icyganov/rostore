package org.rostore.client;

import org.rostore.entity.media.RecordOption;

import java.util.EnumSet;
import java.util.Set;

/**
 * This is a backbone class that contains all the data needed to execute a
 * REST request against the remote rostore instance.
 *
 * <p>This class is used behind the scenes and is usually never used by client explicitly.</p>
 */
public class RequestProperties {

    private String contentType;
    private String path;
    private Long version;
    private Long eol;
    private EnumSet<RecordOption> recordOptions = EnumSet.noneOf(RecordOption.class);
    private String trackingId;

    private final RoStoreClientProperties roStoreClientProperties;

    /**
     * Properties of the {@link RoStoreClient} to be used in the request.
     *
     * @return properties class
     */
    public RoStoreClientProperties getRoStoreClientProperties() {
        return roStoreClientProperties;
    }

    /**
     * Content type of the request (header).
     *
     * @return the content type
     */
    public String getContentType() {
        return contentType;
    }

    public RequestProperties contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * The REST path to execute.
     * @return the request's path
     */
    public String getPath() {
        return path;
    }

    /**
     * The Unix End of Live - used to set the expiration of the key.
     *
     * <p>The value might be null.</p>
     *
     * @return the eol
     */
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
