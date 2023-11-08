package org.rostore.v2.container;

import org.rostore.entity.Record;

public class DataWithRecord<T> {

    private final Record record;
    private final T data;

    public DataWithRecord(final Record record, final T data) {
        this.record = record;
        this.data = data;
    }

    public Record getRecord() {
        return record;
    }

    public T getData() {
        return data;
    }
}
