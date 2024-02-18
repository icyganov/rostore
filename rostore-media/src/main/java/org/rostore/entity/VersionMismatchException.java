package org.rostore.entity;

import org.rostore.Utils;
import org.rostore.entity.media.RecordOption;

/**
 * Exception thrown when the version of the record in the store is
 * not the one as requested.
 */
public class VersionMismatchException extends RoStoreException {

    private static String getVersionString(long version) {
        if (version == Utils.VERSION_UNDEFINED) {
            return "#undefined";
        } else {
            return String.valueOf(version);
        }
    }

    private VersionMismatchException(final long dataVersion, final long queryVersion) {
        super("The rostore data version is " + getVersionString(dataVersion) + ", the query data version is " + getVersionString(queryVersion));
    }

    /**
     * Checks if the version of the request fits the underlying stored version.
     * If not exception is thrown.
     * <p>It may ignore the versioning if the version is can be overridden as set in the {@link RecordOption#OVERRIDE_VERSION} in {@link Record}.</p>
     *
     * @param dataVersion the version of the data in the store
     * @param queryRecord a record as it should be stored
     * @throws VersionMismatchException the exception
     */
    public static void checkAndThrow(final long dataVersion, final Record queryRecord) throws VersionMismatchException {
        checkAndThrow(dataVersion, queryRecord.getVersion(), queryRecord.hasOption(RecordOption.OVERRIDE_VERSION));
    }

    /**
     * Checks if the version of the request fits the underlying stored version.
     * If not exception is thrown.
     * <p>It may ignore the versioning if the version is just overridden.</p>
     *
     * @param dataVersion the version of the entry as stored in the store
     * @param queryVersion the version as provided in the request
     * @param overrideVersion {@code true} if no checks should be done
     * @throws VersionMismatchException the exception
     */
    public static void checkAndThrow(final long dataVersion, final long queryVersion, final boolean overrideVersion) throws VersionMismatchException {
        if (!overrideVersion) {
            if (dataVersion != Utils.VERSION_UNDEFINED) {
                // the data in the store is versioned
                if (queryVersion != Utils.VERSION_UNDEFINED) {
                    // the incoming is versioned and the store version is versioned, they must be the same!
                    if (dataVersion != queryVersion) {
                        throw new VersionMismatchException(dataVersion, queryVersion);
                    }
                }
            } else {
                // data is the store is unversioned
                if (queryVersion != Utils.VERSION_UNDEFINED) {
                    // query is versioned => must be an error
                    throw new VersionMismatchException(dataVersion, queryVersion);
                }
            }
        }
    }
}
