package org.rostore.entity;

public class Entity {
    int dataId;
    int expiration;
    int offset;

    public Entity(int offset, int dataId, int expiration) {
        this.offset = offset;
        this.dataId = dataId;
        this.expiration = expiration;
    }

    public int getDataId() {
        return dataId;
    }

    public int getExpiration() {
        return expiration;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
