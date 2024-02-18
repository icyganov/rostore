package org.rostore.entity;

import org.rostore.Utils;
import org.rostore.entity.media.RecordOption;

/**
 * Thrown when the version of the entry is set incorrectly on the entry initialization.
 * <p>It either should be {@link Utils#VERSION_UNDEFINED} for non-versioned entries or
 * it should be {@link Utils#VERSION_START}.</p>
 * <p>The logic can be overridden if the {@link RecordOption#OVERRIDE_VERSION} is provided to the record.</p>
 */
public class VersionMismatchInitException extends RoStoreException {

    private VersionMismatchInitException(final long queryVersion) {
        super("The query data version for new entry must be strictly " + Utils.VERSION_START + ", provided=" + queryVersion);
    }

    /**
     * Checks if the record can be initialized with the given set of {@link RecordOption} and the version.
     * If the conditions are not satisfied an exception is thrown.
     *
     * @param record the record to analyze
     * @throws VersionMismatchInitException this exception
     */
    public static void checkAndThrow(final Record record) throws VersionMismatchInitException {
        if (!record.hasOption(RecordOption.OVERRIDE_VERSION)) {
            if (record.getVersion() != Utils.VERSION_UNDEFINED) {
                // the version is specified!
                if (record.getVersion() != Utils.VERSION_START) {
                    // the initial version should all the time be the same
                    throw new VersionMismatchInitException(record.getVersion());
                }
            }
        }
    }
}
