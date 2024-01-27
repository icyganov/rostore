package org.rostore.v2.container;

import org.rostore.entity.Record;

/**
 * Represents a combination of the value data and the metadata associated with the entry
 * @param <T>
 */
public class DataWithRecord<T> {

    private final Record record;
    private final T data;

    public DataWithRecord(final Record record, final T data) {
        this.record = record;
        this.data = data;
    }

    /**
     * Provides a metadata of the entry
     *
     * @return the record
     */
    public Record getRecord() {
        return record;
    }

    /**
     * Provides the value data
     *
     * @return the value's data
     */
    public T getData() {
        return data;
    }
}
