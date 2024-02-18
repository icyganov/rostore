package org.rostore.entity;

import org.rostore.Utils;
import org.rostore.entity.media.RecordOption;

import java.util.EnumSet;
import java.util.Set;

/**
 * Record describes the payload associated with the key in the storage.
 * <p>It contains the metadata like TTL, version and id.</p>
 * <p>id is an arbitrary long, but is usually used to reference the
 * block index where the value is stored.</p>
 * <p>Record also contains the set of options that should be considered on
 * the key-specific operations.</p>
 */
public class Record {

    private long id = Utils.ID_UNDEFINED;
    private long eol = Utils.EOL_FOREVER;
    private long version = Utils.VERSION_UNDEFINED;
    private Set<RecordOption> options = EnumSet.noneOf(RecordOption.class);

    /**
     * Creates a record based on ttl or unixEol provided.
     * <p>This is a helper function and can be used in the situations when either/or of these to parametrrs are given.</p>
     * @param ttl a ttl - time to live or {@code null}
     * @param unixEol a unix timestamp until this record should be valid
     * @return the record object
     */
    public Record ttlOrUnitEol(final Long ttl, final Long unixEol) {
        if (unixEol != null) {
            this.eol = Utils.unixEol2eol(unixEol);
            if (eol < 0) {
                throw new EOLIncorrectException(eol);
            }
            return this;
        }
        if (ttl != null) {
            this.eol = Utils.ttl2eol(ttl);
            return this;
        }
        this.eol = Utils.EOL_FOREVER;
        return this;
    }

    /**
     * The Id associated with the key-value pair.
     *
     * @return the id
     */
    public long getId() {
        return id;
    }

    /**
     * Sets the id to be stored with the record.
     *
     * @param id the id
     * @return the record object
     */
    public Record id(long id) {
        this.id = id;
        return this;
    }

    /**
     * Calculates the end of life in terms of unix epoc
     *
     * @return the unix timestamp when the record will expire or 0 if the object is not expected to expire
     */
    public long getUnixEol() {
        return Utils.eol2unix(eol);
    }

    /**
     * Gets ttl is seconds
     * @return the ttl in seconds
     */
    public long getTtl() {
        return Utils.eol2ttl(eol);
    }

    /**
     * Returns end of life as it stored in the record
     *
     * @return the eof of the record
     */
    public long getEol() {
        return eol;
    }

    public Record ttl(final long ttl) {
        this.eol = Utils.ttl2eol(ttl);
        return this;
    }

    /**
     * Sets end of live of the record
     *
     * @param eol the eol of the record
     * @return the record object
     */
    public Record eol(final long eol) {
        this.eol = eol;
        return this;
    }

    /**
     * Increment version
     * <p>Function will trim the counter by the number of bytes</p>
     * @param lengthBytes the number of bytes to trim the version
     */
    public void incrementVersion(final int lengthBytes) {
        if (version != Utils.VERSION_UNDEFINED) {
            version++;
            version = Utils.trimByBytes(version, lengthBytes);
            if (version == Utils.VERSION_UNDEFINED) {
                version+=2;
            }
        }
    }

    /**
     * Provides a version as set to record
     *
     * @return the version
     */
    public long getVersion() {
        return version;
    }

    /**
     * Sets a version to the record
     *
     * @param version a version to be set
     * @return the record object
     */
    public Record version(final long version) {
        this.version = version;
        return this;
    }

    /**
     * Checks if the record contains a provided option
     *
     * @param option an option to lookup
     * @return {@code true} if option is available
     */
    public boolean hasOption(final RecordOption option) {
        return options.contains(option);
    }

    /**
     * Adds an option to the record object
     * @param recordOption an option to add
     * @return the record object
     */
    public Record addOption(final RecordOption recordOption) {
        this.options.add(recordOption);
        return this;
    }

    /**
     * Adds a set of options to the record object
     * @param recordOptions an option set to add
     * @return the record object
     */
    public Record addOptions(final Set<RecordOption> recordOptions) {
        if (recordOptions != null) {
            this.options.addAll(recordOptions);
        }
        return this;
    }
}
