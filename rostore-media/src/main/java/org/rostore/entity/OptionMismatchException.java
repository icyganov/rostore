package org.rostore.entity;

import org.rostore.entity.media.RecordOption;
import org.rostore.v2.keys.KeyBlockEntry;

/**
 * Exception thrown when an operation on a key can't be executed as requested
 */
public class OptionMismatchException extends RoStoreException {

    private final Record record;

    /**
     * Record caused an exception (as provided when operation is executed)
     * @return the record
     */
    public Record getRecord() {
        return record;
    }

    /**
     * Checks options when an entry should be updated and throws an exception if the state is invalid (update is not allowed).
     *
     * @param entry an entry where the operation should be executed
     * @param record a record with options as it expected to be executed
     */
    public static void checkUpdateRecord(final KeyBlockEntry entry, final Record record) {
        if (record.hasOption(RecordOption.ONLY_INSERT)) {
            if (!entry.isExpired()) {
                throw new OptionMismatchException("Record id=" + record.getId() + " can't be inserted, but replaced.", record);
            }
        }
    }

    /**
     * Checks options when an entry should be inserted and throws an exception if the state is invalid (insert is not allowed).
     *
     * @param record a record with options as it expected to be inserted
     */
    public static void checkInsertRecord(final Record record) {
        if (record.hasOption(RecordOption.ONLY_REPLACE)) {
            throw new OptionMismatchException("Record id=" + record.getId() + " can't be replaced, but only inserted.", record);
        }
    }

    private OptionMismatchException(final String message, final Record record) {
        super(message);
        this.record = record;
    }

}
