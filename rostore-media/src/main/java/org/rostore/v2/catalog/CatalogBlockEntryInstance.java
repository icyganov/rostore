package org.rostore.v2.catalog;

import org.rostore.entity.RoStoreException;

public class CatalogBlockEntryInstance {

    final CatalogBlockEntry catalogBlockEntry;
    final private long entryHash;
    final private long start;
    final private long stop;

    private void throwExceptionIfInvalid(final String message) {
        if (invalid()) {
            throw new RoStoreException("The <" + this + "> is invalid: " + message);
        }
    }

    public long getStart() {
        throwExceptionIfInvalid("getStart");
        return start;
    }

    public long getStop() {
        throwExceptionIfInvalid("getStop");
        return stop;
    }

    public boolean valid() {
        return entryHash != -1;
    }

    public boolean invalid() {
        return entryHash == -1;
    }

    public CatalogBlockEntryInstance(final CatalogBlockEntry catalogBlockEntry) {
        this.entryHash = catalogBlockEntry.getHash();
        this.start = catalogBlockEntry.getEntryStart();
        this.stop = catalogBlockEntry.getEntryStop();
        this.catalogBlockEntry = catalogBlockEntry;
    }

    public CatalogBlockEntryInstance(final CatalogBlockEntry catalogBlockEntry, long hash) {
        this.entryHash = hash;
        this.catalogBlockEntry = catalogBlockEntry;
        if (hash != -1) {
            catalogBlockEntry.moveToHash(hash);
            this.start = catalogBlockEntry.getEntryStart();
            this.stop = catalogBlockEntry.getEntryStop();
        } else {
            start = -1;
            stop = -1;
        }
    }

    public void restore() {
        catalogBlockEntry.moveToHash(entryHash);
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder("CatalogBlockEntryInstance: ");
        if (invalid()) {
            sb.append("invalid");
        } else {
            sb.append("start=");
            sb.append(start);
            sb.append(", stop=");
            sb.append(stop);
        }
        return sb.toString();
    }
}
