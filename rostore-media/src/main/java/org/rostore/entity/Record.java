package org.rostore.entity;

import org.rostore.Utils;
import org.rostore.entity.media.RecordOption;

import java.util.EnumSet;
import java.util.Set;

public class Record {

    private long id = Utils.ID_UNDEFINED;
    private long eol = Utils.EOL_FOREVER;
    private long version = Utils.VERSION_UNDEFINED;
    private Set<RecordOption> options = EnumSet.noneOf(RecordOption.class);

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

    public long getId() {
        return id;
    }

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
