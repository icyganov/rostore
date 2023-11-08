package org.rostore.mapper;

public class MapperProperties {

    private int bytesPerBlockIndex;
    private int bytesPerBlockOffset;

    public int getBytesPerBlockIndex() {
        return bytesPerBlockIndex;
    }

    public void setBytesPerBlockIndex(int bytesPerBlockIndex) {
        this.bytesPerBlockIndex = bytesPerBlockIndex;
    }

    public int getBytesPerBlockOffset() {
        return bytesPerBlockOffset;
    }

    public void setBytesPerBlockOffset(int bytesPerBlockOffset) {
        this.bytesPerBlockOffset = bytesPerBlockOffset;
    }
}
