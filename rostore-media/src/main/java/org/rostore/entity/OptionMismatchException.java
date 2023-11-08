package org.rostore.entity;

import org.rostore.entity.media.RecordOption;
import org.rostore.v2.keys.KeyBlockEntry;

public class OptionMismatchException extends RuntimeException {

    private final Record record;

    public Record getRecord() {
        return record;
    }

    public static void checkUpdateRecord(final KeyBlockEntry entry, final Record record) {
        if (record.hasOption(RecordOption.ONLY_INSERT)) {
            if (!entry.isExpired()) {
                throw new OptionMismatchException("Record id=" + record.getId() + " can't be inserted, but replaced.", record);
            }
        }
    }

    public static void checkInsertRecord(final Record record) {
        if (record.hasOption(RecordOption.ONLY_REPLACE)) {
            throw new OptionMismatchException("Record id=" + record.getId() + " can't be replaced, but only inserted.", record);
        }
    }

    public OptionMismatchException(final String message, final Record record) {
        super(message);
        this.record = record;
    }

}
