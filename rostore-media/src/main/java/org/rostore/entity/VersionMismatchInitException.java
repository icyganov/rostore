package org.rostore.entity;

import org.rostore.Utils;
import org.rostore.entity.media.RecordOption;

public class VersionMismatchInitException extends RoStoreException {

    private VersionMismatchInitException(final long queryVersion) {
        super("The query data version for new entry must be strictly " + Utils.VERSION_START + ", provided=" + queryVersion);
    }

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
