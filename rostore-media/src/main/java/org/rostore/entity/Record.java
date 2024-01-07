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
     * @return
     */
    public long getId() {
        return id;
    }

    /**
     * Sets the id to be stored with the record.
     * @param id the id
     * @return the recocd object
     */
    public Record id(long id) {
        this.id = id;
        return this;
    }

    public long getUnixEol() {
        return Utils.eol2unix(eol);
    }

    public long getTtl() {
        return Utils.eol2ttl(eol);
    }

    public long getEol() {
        return eol;
    }

    public Record ttl(final long ttl) {
        this.eol = Utils.ttl2eol(ttl);
        return this;
    }

    public Record eol(long eol) {
        this.eol = eol;
        return this;
    }

    public void incrementVersion(final int lengthBytes) {
        if (version != Utils.VERSION_UNDEFINED) {
            version++;
            version = Utils.trimByBytes(version, lengthBytes);
            if (version == Utils.VERSION_UNDEFINED) {
                version+=2;
            }
        }
    }

    public long getVersion() {
        return version;
    }

    public Record version(long version) {
        this.version = version;
        return this;
    }

    public boolean hasOption(final RecordOption option) {
        return options.contains(option);
    }

    public Record addOption(final RecordOption recordOption) {
        this.options.add(recordOption);
        return this;
    }

    public Record addOptions(final Set<RecordOption> recordOptions) {
        if (recordOptions != null) {
            this.options.addAll(recordOptions);
        }
        return this;
    }
}
