package org.rostore.v2.keys;

import org.rostore.Utils;
import org.rostore.v2.media.MediaProperties;

public class RecordLengths {

    private final int idLength;
    private final int eolLength;
    private final int versionLength;

    public static final RecordLengths standardRecordLengths(final MediaProperties mediaProperties) {
        return new RecordLengths(mediaProperties.getMapperProperties().getBytesPerBlockIndex(),
                Utils.BYTES_PER_EOL,
                Utils.BYTES_PER_VERSION);
    }

    public RecordLengths(int idLength, int eolLength, int versionLength) {
        this.idLength = idLength;
        this.eolLength = eolLength;
        this.versionLength = versionLength;
    }

    public int getIdLength() {
        return idLength;
    }

    public int getEolLength() {
        return eolLength;
    }

    public int getVersionLength() {
        return versionLength;
    }

    public int getTotalLength() {
        return idLength + eolLength + versionLength;
    }
}
