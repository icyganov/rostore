package org.rostore.mapper;

/**
 * Class defines the properties of the {@link BinaryMapper}
 * that specify the length of the fields depending on the storage internal properties.
 */
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
